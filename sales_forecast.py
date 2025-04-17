import pandas as pd
import requests
import time
from datetime import datetime
import numpy as np
from sklearn.metrics import mean_squared_error  # 引入誤差評估函式
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe

SHEET_ID = "1ufAI8OY64NKrpLS17qlYuOX5HWgGlCyZsgPRC-NfQJI"
SHEET_NAME = "sheet1"
MODEL_DESCRIPTION_SHEET_NAME = "model_description"

# === 設定 Google Sheet ===
SHEET_ID = "1ufAI8OY64NKrpLS17qlYuOX5HWgGlCyZsgPRC-NfQJI"
SHEET_NAME = "sheet1"
REPORT_SHEET_NAME = "report"
NEW_SKU_SHEET_NAME = "new_skus"
MODEL_DESCRIPTION_SHEET_NAME = "model_description"

# === 從環境變數讀取 Slack Webhook URL ===
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# === Function：發送 Slack 通知 ===
def send_slack_message(message):
    if not SLACK_WEBHOOK_URL:
        print("⚠️ Slack Webhook URL 未設置，略過通知。")
        return
    payload = {"text": message}
    try:
        requests.post(SLACK_WEBHOOK_URL, json=payload)
    except Exception as e:
        print(f"⚠️ Slack 發送失敗：{e}")

# === Function：API 重試機制 ===
def fetch_data_with_retry(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"⚠️ API 失敗 {attempt + 1}/{max_retries} 次: {e}")
            time.sleep(2)
    send_slack_message(f"🚨 [預測系統] API 資料取得失敗：{url}")
    raise Exception(f"🚨 API 失敗超過 {max_retries} 次：{url}")

# === Function：欄位防呆 ===
def ensure_columns(df, columns):
    for col in columns:
        if col not in df.columns:
            print(f"⚠️ DataFrame 缺少欄位：{col}，自動補 0")
            df[col] = 0
    return df

# === Function：寫入 Google Sheet ===
def write_to_gsheet(df, sheet_id, sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file("besparks-service-account.json", scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(sheet_name)
    worksheet.clear()
    set_with_dataframe(worksheet, df)

# === 主流程 ===
try:
   sales_data = fetch_data_with_retry("https://api.besparks.co/api:074LNDs2/data/sales_history")
   product_data = fetch_data_with_retry("https://api.besparks.co/api:074LNDs2/data/product_info")


   sales_df = pd.DataFrame(sales_data)
   product_df = pd.DataFrame(product_data)
   forecast_df = pd.DataFrame()


   # 數值轉換
   sales_df['quantity_sold'] = pd.to_numeric(sales_df['quantity_sold'], errors='coerce')
   product_df['price'] = pd.to_numeric(product_df['price'], errors='coerce')
   product_df['gross_margin'] = pd.to_numeric(product_df['gross_margin'], errors='coerce')


   # 商品篩選
   product_df['type'] = product_df['type'].astype(str).str.lower()
   product_df['is_tangible'] = product_df['is_tangible'].astype(str).str.lower() == 'true'
   product_df = product_df[
       (product_df['type'] == 'single') &
       (product_df['is_tangible']) &
       (product_df['status'] != 'archived') &
       (~product_df['product_line'].isin(['Accessories', 'Others', 'Packaging', 'Raw Materials', 'Promotion Bundle']))
   ].copy()


   sales_df['date'] = pd.to_datetime(sales_df['date'], format='%Y-%m')
   latest_date = sales_df['date'].max()


   # 聚合為 SKU × 月維度 + B2C 篩選
   # sales_df = sales_df[sales_df['channel_type'] == 'B2C']
   sales_df['month'] = sales_df['date'].dt.to_period('M')
   monthly_sales = sales_df.groupby(['sku', 'month'])['quantity_sold'].sum().reset_index()
   monthly_sales['date'] = monthly_sales['month'].dt.to_timestamp()


   # 合併產品資料
   full_df = monthly_sales.merge(product_df, on='sku', how='inner')


   # 預測公式
   def weighted_average(group, weights):
       # 排除當月資料以免低估
       historical = group[group['date'] < latest_date]
       recent_1m = historical[historical['date'] == latest_date - pd.DateOffset(months=1)]['quantity_sold'].sum()
       recent_3m = historical[historical['date'] >= latest_date - pd.DateOffset(months=3)]['quantity_sold'].mean()
       recent_6m = historical[historical['date'] >= latest_date - pd.DateOffset(months=6)]['quantity_sold'].mean()
       return weights[0] * recent_1m + weights[1] * recent_3m + weights[2] * recent_6m
  # === 回測函數 ===
   def backtest(df, weights, split_date):
      train = df[df['date'] < split_date].copy()
      test = df[df['date'] >= split_date].copy()
        
      predictions = train.groupby('sku').apply(lambda x: weighted_average(x, weights)).reset_index(name='prediction')
        
      merged = test.merge(predictions, on='sku', how='left').fillna(0)  # 處理沒有預測值的 SKU
      rmse = np.sqrt(mean_squared_error(merged['quantity_sold'], merged['prediction']))
      return rmse

   # === 權重組合 ===
   weight_combinations = [
      (0.5, 0.3, 0.2),
      (0.6, 0.2, 0.2),
      (0.4, 0.4, 0.2),
      (0.3, 0.3, 0.4),
      (0.7, 0.2, 0.1),
      (0.1, 0.1, 0.8) # 增加多組權重
    ]

   # === 設定訓練集和測試集分割日期 ===
   latest_date = sales_df['date'].max()
   split_date = latest_date - pd.DateOffset(months=6)  # 例如，最後 6 個月作為測試集

   # === 迴圈計算並評估 ===
   results = []
   for weights in weight_combinations:
       rmse = backtest(full_df, weights, split_date)
       results.append({'weights': weights, 'rmse': rmse})

   results_df = pd.DataFrame(results)
   best_weights = results_df.loc[results_df['rmse'].idxmin()]['weights']  # 找到 RMSE 最小的權重組合

   print("回測結果:")
   print(results_df)
   print("\n最佳權重:", best_weights)

   # === 使用最佳權重進行最終預測 ===
   forecast_df = full_df.groupby('sku').apply(lambda x: weighted_average(x, best_weights)).reset_index(name='base_forecast')

   # Debug: 查看特定 SKU 預測來源細節
   sku_to_check = 'BLK-P0001'
   sku_group = full_df[full_df['sku'] == sku_to_check].copy()
   print(f"=== ⏱ {sku_to_check} 原始近 6 個月彙總銷售 ===")
   print(sku_group.sort_values('date')[['date', 'quantity_sold']].tail(6))


   historical = sku_group[sku_group['date'] < latest_date]
   q = historical['quantity_sold']
   recent_1m = q[historical['date'] == latest_date - pd.DateOffset(months=1)].sum()
   recent_3m = q[historical['date'] >= latest_date - pd.DateOffset(months=3)].mean()
   recent_6m = q[historical['date'] >= latest_date - pd.DateOffset(months=6)].mean()


   weighted = 0.5 * recent_1m + 0.3 * recent_3m + 0.2 * recent_6m
   print(f"🔍 {sku_to_check} 加權計算細節：")
   print(f" 近1個月總和: {recent_1m:.2f}")
   print(f" 近3個月平均: {recent_3m:.2f}")
   print(f" 近6個月平均: {recent_6m:.2f}")
   print(f" ➡️ 預測加權值（未調整）: {weighted:.2f}")
   forecast_df = forecast_df.merge(product_df[['sku', 'gross_margin', 'product_line', 'type']], on='sku', how='left')


   def adjust_margin(row):
       if pd.isna(row['base_forecast']) or pd.isna(row['gross_margin']):
           return 0
       elif row['gross_margin'] > 0.6:
           return row['base_forecast'] * 1.2
       elif row['gross_margin'] > 0.3:
           return row['base_forecast'] * 1.1
       else:
           return row['base_forecast']


   forecast_df['adjusted_forecast'] = forecast_df.apply(adjust_margin, axis=1).fillna(0).round().astype(int)


   # fallback：新品補推
   forecast_df['sku'] = forecast_df['sku'].fillna('')
   product_df['sku'] = product_df['sku'].fillna('')
   known_skus = set(forecast_df['sku'].dropna())
   all_skus = set(product_df['sku'].dropna())
   new_skus = list(all_skus - known_skus)


   new_sku_df = product_df[product_df['sku'].isin(new_skus)].copy()
   category_avg = forecast_df.groupby('product_line')['adjusted_forecast'].mean().to_dict()
   new_sku_df['adjusted_forecast'] = new_sku_df['product_line'].map(category_avg).fillna(10).round().astype(int)


   # 下一個月標籤
   next_month = datetime.today().replace(day=1) + pd.DateOffset(months=1)
   date_label = next_month.strftime('%Y-%m')
   forecast_df[date_label] = forecast_df['adjusted_forecast']
   new_sku_df[date_label] = new_sku_df['adjusted_forecast']


   # 合併 + 輸出主預測表
   final_df = pd.concat([forecast_df, new_sku_df])[['sku', 'type', 'product_line', date_label]]
   write_to_gsheet(final_df, SHEET_ID, SHEET_NAME)


   # QA 稽核表
   all_forecast = pd.concat([forecast_df, new_sku_df]).copy()
   all_forecast['fallback_used'] = all_forecast['sku'].isin(new_sku_df['sku'])
   all_forecast['forecast_type'] = all_forecast['fallback_used'].map(lambda x: 'fallback' if x else 'historical')
   qa_export = all_forecast[['sku', 'product_line', date_label, 'forecast_type']].copy()
   qa_export = qa_export.rename(columns={date_label: 'forecast_value'})
   write_to_gsheet(qa_export, SHEET_ID, 'qa_audit')


   # 預測邏輯說明表
   model_description_df = pd.DataFrame({
       '項目': ['預測邏輯', '使用特徵', '特殊規則', '資料處理', '預測期間', '數據來源'],
       '說明': [
           '加權移動平均法（近1月60%，近3月30%，近6月10%）',
           'sku, quantity_sold, gross_margin',
           '毛利率調整（高+20%、中+10%、低不變）',
           '過濾 B2C + 月彙總 + fallback 新品平均',
           f'預測月份：{date_label}',
           'API：sales_history, product_info'
       ]
   })
   write_to_gsheet(model_description_df, SHEET_ID, MODEL_DESCRIPTION_SHEET_NAME)


   send_slack_message(f"✅ [預測系統] 已完成 {date_label} 預測，主表與 QA 報表已寫入 Google Sheet")


except Exception as e:
   print(f"Error: {e}")
   send_slack_message(f"🚨 [預測系統] 執行錯誤：{str(e)}")
