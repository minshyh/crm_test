# -*- coding: utf-8 -*-
"""
Poya Sales Spider - 寶雅銷售資料自動化抓取工具
適用於 GitHub Actions 自動部署運行
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Union

import requests
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 設定日誌系統
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class PoyaSalesSpider:
    """寶雅銷售資料爬蟲類別"""
    
    def __init__(self):
        """初始化爬蟲設定"""
        # 從環境變數讀取設定
        self.enable_sheet = self._str_to_bool(os.environ.get("ENABLE_WRITE_TO_SHEET", "True"))
        self.enable_xano = self._str_to_bool(os.environ.get("ENABLE_POST_TO_XANO", "True"))
        self.mode = os.environ.get("SCRAPE_MODE", "daily").lower()  # "daily" or "backfill"
        self.backfill_start = os.environ.get("BACKFILL_START_DATE", "")
        
        # 敏感設定從環境變數讀取
        self.slack_webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
        self.xano_endpoint = os.environ.get("XANO_ENDPOINT", "")
        self.spreadsheet_id = os.environ.get("SPREADSHEET_ID", "")
        
        # 寶雅登入資訊
        self.poya_account = os.environ.get("POYA_ACCOUNT", "")
        self.poya_password = os.environ.get("POYA_PASSWORD", "")
        self.poya_auth_pwd = os.environ.get("POYA_AUTH_PWD", "")
        
        # URL設定
        self.login_url = "https://order.poya.com.tw/LoginCom.aspx"
        self.query_url = "https://order.poya.com.tw/SaleGenQueryAll.aspx"
        
        # 初始化日期範圍
        self._setup_date_range()
        
        # 初始化Google Sheet (如果啟用)
        if self.enable_sheet:
            self._setup_google_sheet()
        
        # 建立Session與重試機制
        self.session = self._create_retry_session()
        
    def _str_to_bool(self, value: str) -> bool:
        """將字串轉換為布林值"""
        return value.lower() in ('true', 'yes', '1', 't', 'y')
    
    def _setup_date_range(self) -> None:
        """設定日期範圍"""
        if self.mode == "daily":
            self.start_date = self.end_date = datetime.today() - timedelta(days=1)
        elif self.mode == "backfill":
            if not self.backfill_start:
                # 預設回填一週
                self.start_date = datetime.today() - timedelta(days=7)
            else:
                try:
                    self.start_date = datetime.strptime(self.backfill_start, "%Y-%m-%d")
                except ValueError:
                    logger.error("回填日期格式錯誤，應為 YYYY-MM-DD")
                    self.start_date = datetime.today() - timedelta(days=7)
            
            self.end_date = datetime.today() - timedelta(days=1)
        else:
            logger.warning(f"未知模式: {self.mode}，使用預設每日模式")
            self.start_date = self.end_date = datetime.today() - timedelta(days=1)
    
    def _setup_google_sheet(self) -> None:
        """設定Google Sheet連接"""
        try:
            # 使用環境變數或文件的服務帳號
            if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
                # 使用環境變數指定的文件路徑
                creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
                self.creds = Credentials.from_service_account_file(
                    creds_path,
                    scopes=["https://www.googleapis.com/auth/spreadsheets"]
                )
            elif "GOOGLE_CREDENTIALS_JSON" in os.environ:
                # 直接使用環境變數中的JSON內容
                creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS_JSON", "{}"))
                self.creds = Credentials.from_service_account_info(
                    creds_json,
                    scopes=["https://www.googleapis.com/auth/spreadsheets"]
                )
            else:
                logger.error("未找到Google服務憑證，無法使用Google Sheet功能")
                self.enable_sheet = False
                return
                
            self.gc = gspread.authorize(self.creds)
            self.sheet = self.gc.open_by_key(self.spreadsheet_id)
            logger.info("Google Sheet 設定成功")
        except Exception as e:
            logger.error(f"Google Sheet 設定失敗: {e}")
            self.enable_sheet = False
    
    def _create_retry_session(self) -> requests.Session:
        """建立帶有重試機制的Session"""
        session = requests.Session()
        retry_strategy = Retry(
            total=5,  # 總重試次數
            backoff_factor=1,  # 重試延遲指數
            status_forcelist=[429, 500, 502, 503, 504],  # 需要重試的HTTP狀態碼
            allowed_methods=["GET", "POST"]  # 允許重試的HTTP方法
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def fetch_sales_data(self, date_str: str) -> Optional[pd.DataFrame]:
        """擷取指定日期的寶雅銷售資料"""
        try:
            # 第一步：登入寶雅系統
            logger.info(f"正在登入寶雅系統...")
            res = self.session.get(self.login_url)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # 取得表單隱藏欄位值
            viewstate = soup.find("input", {"name": "__VIEWSTATE"})["value"]
            eventvalidation = soup.find("input", {"name": "__EVENTVALIDATION"})["value"]
            
            # 檢查憑證是否設置
            if not all([self.poya_account, self.poya_password, self.poya_auth_pwd]):
                logger.error("寶雅帳號密碼未設定，請確認環境變數")
                return None
            
            # 提交登入表單
            payload = {
                '__VIEWSTATE': viewstate,
                '__EVENTVALIDATION': eventvalidation,
                'Account': self.poya_account,
                'Pwd': self.poya_password,
                'AuthPwd': self.poya_auth_pwd,
                'btnLogin': '身份驗證'
            }
            res = self.session.post(self.login_url, data=payload)
            
            # 檢查登入結果
            if "Default.aspx" not in res.url:
                logger.error("寶雅系統登入失敗，請檢查帳號密碼")
                return None
            
            logger.info("成功登入寶雅系統")
            
            # 第二步：訪問查詢頁面
            logger.info(f"正在獲取 {date_str} 資料...")
            res = self.session.get(self.query_url)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # 取得查詢表單隱藏欄位值
            viewstate = soup.find("input", {"name": "__VIEWSTATE"})["value"]
            eventvalidation = soup.find("input", {"name": "__EVENTVALIDATION"})["value"]
            viewstate_gen = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})["value"]
            
            # 提交查詢表單
            payload = {
                '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': viewstate_gen,
                '__EVENTVALIDATION': eventvalidation,
                '__EVENTTARGET': '',
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                'ddlType': '1',
                'EcrDate1': date_str,
                'EcrDate2': date_str,
                'chkSum': 'on',
                'GroupType': 'RBtnPos',
                'btnSearch': '查詢'
            }
            
            # 提交查詢
            res = self.session.post(self.query_url, data=payload)
            
            # 等待查詢結果
            # 寶雅系統較慢，增加等待時間
            time.sleep(15)
            
            # 解析查詢結果
            soup = BeautifulSoup(res.text, 'html.parser')
            table = soup.find('table', {'id': 'dgProd'})
            if not table:
                logger.warning(f"{date_str} 查無資料")
                return None
            
            # 解析表格數據
            rows = table.find_all('tr')
            data = []
            for i, row in enumerate(rows):
                cols = [col.get_text(strip=True) for col in row.find_all('td')]
                if len(cols) == 6:
                    data.append(cols)
            
            if not data:
                logger.warning(f"{date_str} 資料表格為空")
                return None
                
            # 轉換為DataFrame
            columns = ['廠商名稱', '店內碼', '國際條碼', '商品名稱', '銷售量', '庫存量']
            df = pd.DataFrame(data, columns=columns)
            logger.info(f"成功獲取 {date_str} 資料，共 {len(df)} 筆記錄")
            return df
            
        except Exception as e:
            logger.error(f"擷取資料失敗: {str(e)}")
            # 做一個簡單的截斷，避免長時間重試
            time.sleep(5)
            return None
    
    def write_to_sheet(self, sheet_name: str, df: pd.DataFrame) -> bool:
        """寫入資料到Google Sheet"""
        if not self.enable_sheet:
            return False
            
        try:
            logger.info(f"寫入資料至Google Sheet: {sheet_name}")
            # 檢查工作表是否已存在
            worksheet_list = [ws.title for ws in self.sheet.worksheets()]
            if sheet_name in worksheet_list:
                self.sheet.del_worksheet(self.sheet.worksheet(sheet_name))
                
            # 增加新工作表並寫入資料
            worksheet = self.sheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
            set_with_dataframe(worksheet, df)
            logger.info(f"成功寫入 {len(df)} 筆資料至工作表 {sheet_name}")
            return True
        except Exception as e:
            logger.error(f"寫入Google Sheet失敗: {str(e)}")
            return False
    
    def post_to_xano(self, date_str: str, df: pd.DataFrame) -> bool:
        """發送資料到Xano API"""
        if not self.enable_xano or not self.xano_endpoint:
            return False
            
        try:
            # 過濾銷售量大於0的資料
            df["銷售量"] = pd.to_numeric(df["銷售量"], errors="coerce")
            filtered = df[df["銷售量"] > 0].copy()
            
            if filtered.empty:
                logger.info(f"{date_str} 無銷售資料，不發送到Xano")
                return False
                
            # 準備API請求資料
            date_obj = datetime.strptime(date_str.replace("/", "-"), "%Y-%m-%d")
            order_no = date_obj.strftime("poya%Y%m%d080000")
            timestamp_str = datetime.now().isoformat()
            
            order_items = [
                {"barcode": row["國際條碼"], "sales_qty": int(row["銷售量"])}
                for _, row in filtered.iterrows()
            ]
            
            payload = {
                "data": {
                    "channel_id": 170,
                    "channel_order_no": order_no,
                    "orders": order_items,
                    "timestamp": timestamp_str
                }
            }
            
            # 發送API請求
            logger.info(f"發送 {len(order_items)} 筆資料到Xano...")
            res = requests.post(self.xano_endpoint, json=payload)
            
            if res.status_code == 200:
                logger.info(f"成功發送資料到Xano，狀態碼: {res.status_code}")
                return True
            else:
                logger.error(f"Xano API返回錯誤: {res.status_code}, {res.text}")
                return False
                
        except Exception as e:
            logger.error(f"發送資料到Xano失敗: {str(e)}")
            return False
    
    def send_slack_message(self, message: str) -> bool:
        """發送通知到Slack"""
        if not self.slack_webhook:
            return False
            
        try:
            payload = {"text": message}
            res = requests.post(
                self.slack_webhook, 
                data=json.dumps(payload), 
                headers={"Content-Type": "application/json"}
            )
            
            if res.status_code == 200:
                logger.info(f"成功發送Slack通知")
                return True
            else:
                logger.error(f"Slack通知發送失敗: {res.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Slack通知發送失敗: {str(e)}")
            return False
    
    def run(self) -> Dict[str, str]:
        """運行爬蟲主流程"""
        results = {}
        current_date = self.start_date
        
        # 檢查必要憑證
        if not all([self.poya_account, self.poya_password, self.poya_auth_pwd]):
            error_msg = "寶雅帳號密碼未設定，無法執行爬蟲"
            logger.error(error_msg)
            self.send_slack_message(f"❌ {error_msg}")
            return {"error": error_msg}
        
        logger.info(f"開始爬取日期範圍: {self.start_date.strftime('%Y-%m-%d')} 至 {self.end_date.strftime('%Y-%m-%d')}")
        
        # 逐日執行
        while current_date <= self.end_date:
            date_str = current_date.strftime("%Y/%m/%d")
            sheet_name = current_date.strftime("%Y-%m-%d")
            logger.info(f"📆 處理日期: {sheet_name}")
            
            try:
                # 爬取當日資料
                df = self.fetch_sales_data(date_str)
                
                if df is not None and not df.empty:
                    # 寫入Google Sheet
                    if self.enable_sheet:
                        sheet_result = self.write_to_sheet(sheet_name, df)
                    
                    # 發送到Xano
                    if self.enable_xano:
                        xano_result = self.post_to_xano(sheet_name, df)
                        if xano_result:
                            df["銷售量"] = pd.to_numeric(df["銷售量"], errors="coerce")
                            filtered = df[df["銷售量"] > 0]
                            self.send_slack_message(f"✅ {sheet_name} 匯入成功，共 {len(filtered)} 筆")
                            results[sheet_name] = f"成功: {len(filtered)} 筆"
                        else:
                            self.send_slack_message(f"⚠️ {sheet_name} Xano 匯入失敗")
                            results[sheet_name] = "Xano匯入失敗"
                else:
                    msg = f"⚠️ {sheet_name} 查無資料"
                    logger.warning(msg)
                    self.send_slack_message(msg)
                    results[sheet_name] = "查無資料"
            
            except Exception as e:
                error_msg = f"❌ {sheet_name} 處理失敗: {str(e)}"
                logger.error(error_msg)
                self.send_slack_message(f"❌ {sheet_name} 爬取寶雅訂單失敗!")
                results[sheet_name] = "錯誤"
            
            current_date += timedelta(days=1)
        
        # 完成所有處理
        logger.info("爬蟲任務執行完成")
        return results


def main():
    """主程式入口"""
    logger.info("寶雅銷售爬蟲開始執行")
    
    spider = PoyaSalesSpider()
    results = spider.run()
    
    # 輸出結果摘要
    logger.info("============ 執行結果摘要 ============")
    for date, result in results.items():
        logger.info(f"{date}: {result}")
    
    return results


if __name__ == "__main__":
    main()
