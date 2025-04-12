# 📦 Production Python Script for GitHub Actions - Sales Forecast Model

import pandas as pd
import requests
import time
import numpy as np
import os
from datetime import datetime, timedelta
import xgboost as xgb
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe

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

try:
    print("📥 開始讀取 API 資料...")
    sales_data = fetch_data_with_retry("https://api.besparks.co/api:074LNDs2/data/slaes_history")
    product_data = fetch_data_with_retry("https://api.besparks.co/api:074LNDs2/data/product_info")
    forecast_data = fetch_data_with_retry("https://api.besparks.co/api:074LNDs2/data/forecast")

    sales_df = pd.DataFrame(sales_data)
    product_df = pd.DataFrame(product_data)
    forecast_df = pd.DataFrame(forecast_data)
    sales_df['date'] = pd.to_datetime(sales_df['date'], format='%Y-%m')

    print("✅ API 資料讀取完成")

    print("🧩 數據清洗與特徵工程...")
    product_df = ensure_columns(product_df, ['price', 'sku_cost', 'gross_margin'])
    product_df['price'] = pd.to_numeric(product_df.get('price', product_df.get('msrp', 0)), errors='coerce').fillna(0)
    product_df['sku_cost'] = pd.to_numeric(product_df['sku_cost'], errors='coerce').fillna(0)
    product_df['gross_margin'] = product_df['price'] - product_df['sku_cost']

    merged_df = sales_df.merge(product_df, on='sku', how='left', suffixes=('', '_prod'))
    merged_df = ensure_columns(merged_df, ['price_prod', 'sku_cost_prod', 'gross_margin_prod'])
    merged_df['price'] = merged_df['price_prod'].fillna(0)
    merged_df['sku_cost'] = merged_df['sku_cost_prod'].fillna(0)
    merged_df['gross_margin'] = merged_df['gross_margin_prod'].fillna(0)

    merged_df['month'] = merged_df['date'].dt.month
    merged_df['year'] = merged_df['date'].dt.year
    sku_encoder = LabelEncoder()
    merged_df['sku_encoded'] = sku_encoder.fit_transform(merged_df['sku'])

    merged_df = merged_df.sort_values(['sku', 'date'])
    merged_df['prev_1_month_qty'] = merged_df.groupby('sku')['quantity_sold'].shift(1).fillna(0)
    merged_df['rolling_3_month_qty'] = merged_df.groupby('sku')['quantity_sold'].transform(lambda x: x.shift(1).rolling(3, min_periods=1).mean()).fillna(0)
    merged_df['rolling_6_month_qty'] = merged_df.groupby('sku')['quantity_sold'].transform(lambda x: x.shift(1).rolling(6, min_periods=1).mean()).fillna(0)

    feature_columns = ['month', 'year', 'sku_encoded', 'prev_1_month_qty', 'rolling_3_month_qty', 'rolling_6_month_qty', 'price', 'gross_margin']
    for col in feature_columns:
        merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce').fillna(0)

    print("✅ 特徵工程完成")

    print("🧠 開始模型訓練...")
    dtrain = xgb.DMatrix(merged_df[feature_columns], label=merged_df['quantity_sold'])
    params = {'objective': 'reg:squarederror', 'eval_metric': 'rmse'}
    model = xgb.train(params, dtrain, num_boost_round=100)

    preds_train = model.predict(dtrain)
    mae = mean_absolute_error(merged_df['quantity_sold'], preds_train)
    mape = mean_absolute_percentage_error(merged_df['quantity_sold'], preds_train)

    print(f"✅ 模型訓練完成，MAE: {mae:.2f}, MAPE: {mape:.2%}")

    print("🔮 預測未來三個月...")
    future_months = [datetime.today() + timedelta(days=30 * i) for i in range(1, 4)]
    future_df = pd.DataFrame({'month': [d.month for d in future_months], 'year': [d.year for d in future_months]})
    future_skus = product_df['sku'].unique()
    future_df = future_df.assign(key=1).merge(pd.DataFrame({'sku': future_skus, 'key': 1}), on='key').drop('key', axis=1)

    sku_mapping = dict(zip(sku_encoder.classes_, sku_encoder.transform(sku_encoder.classes_)))
    future_df['sku_encoded'] = future_df['sku'].map(sku_mapping).fillna(-1).astype(int)

    latest_product_info = product_df[['sku', 'price', 'sku_cost', 'gross_margin']].drop_duplicates('sku')
    future_df = future_df.merge(latest_product_info, on='sku', how='left')

    last_sales = merged_df[merged_df['date'] == merged_df['date'].max()][['sku', 'quantity_sold', 'rolling_3_month_qty', 'rolling_6_month_qty']].drop_duplicates('sku')
    last_sales.rename(columns={'quantity_sold': 'prev_1_month_qty'}, inplace=True)
    future_df = future_df.merge(last_sales, on='sku', how='left')

    future_df = ensure_columns(future_df, feature_columns)
    for col in feature_columns:
        future_df[col] = pd.to_numeric(future_df[col], errors='coerce').fillna(0)

    dfmatrix = xgb.DMatrix(future_df[feature_columns])
    future_df['forecast_qty'] = np.maximum(model.predict(dfmatrix).round().astype(int), 0)

    result = future_df.pivot(index='sku', columns='month', values='forecast_qty').reset_index()
    month_map = {m: f'未來{i+1}個月銷售預測' for i, m in enumerate(result.columns[1:])}
    result.rename(columns=month_map, inplace=True)

    print("📝 輸出 Google Sheet 報表...")
    write_to_gsheet(result, SHEET_ID, SHEET_NAME)

    report_df = pd.DataFrame({'指標': ['MAE', 'MAPE'], '數值': [mae, mape]})
    write_to_gsheet(report_df, SHEET_ID, REPORT_SHEET_NAME)

    known_skus = set(merged_df['sku'].unique())
    all_skus = set(product_df['sku'].unique())
    new_skus = all_skus - known_skus
    new_sku_df = pd.DataFrame({'新品 SKU': list(new_skus)})
    write_to_gsheet(new_sku_df, SHEET_ID, NEW_SKU_SHEET_NAME)

    model_description_df = pd.DataFrame({
        '項目': ['模型類型', '預測目標', '使用特徵', '數據來源', '準確率指標', '模型訓練次數', '特別說明'],
        '說明': [
            'XGBoost Regressor',
            'SKU 每月銷量',
            'month, year, sku_encoded, price, gross_margin, 過去1個月銷量, 滾動3/6個月銷量平均',
            'API：sales_history, product_info',
            'MAE / MAPE 已在 report 分頁',
            '100 次迭代 (num_boost_round=100)',
            'SKU 經 LabelEncoder 編碼，價格及毛利為主要影響因子之一'
        ]
    })
    write_to_gsheet(model_description_df, SHEET_ID, MODEL_DESCRIPTION_SHEET_NAME)

    send_slack_message("✅ [預測系統] 最新月份銷售預測已完成並上傳至 Google Sheet！")
    print("✅ 預測流程已完成，請檢查 Google Sheet 報表！")

except Exception as e:
    send_slack_message(f"🚨 [預測系統] 執行過程中發生錯誤：{e}")
    print(f"❌ 發生錯誤：{e}")
