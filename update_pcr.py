import os
import requests
import zipfile
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from openpyxl import load_workbook

# ============ USER INPUT ============
EXISTING_XLSX_PATH = r"C:\Users\10102\Downloads\DAILY UPDATE 2\Nifty_50_PCR_Hisotrical_Data.xlsx"  # <-- PUT YOUR ACTUAL PATH HERE
TEMP_DIR = "temp_udiff_downloads"
PROCESSED_XLSX_PATH = "Updated_PCR_Output.xlsx"  # Final PCR output (can be same as input if overwrite allowed)

NIFTY_50_SYMBOLS = {
    "RELIANCE", "TCS", "HDFC", "INFY", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "BHARTIARTL", "ITC", "LT",
    "ASIANPAINT", "HINDUNILVR", "MARUTI", "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "SBIN", "NTPC",
    "POWERGRID", "ULTRACEMCO", "NESTLEIND", "BRITANNIA", "M&M", "SUNPHARMA", "DIVISLAB", "INDUSINDBK",
    "TATAMOTORS", "TITAN", "DRREDDY", "GRASIM", "ADANIPORTS", "ADANIENT", "ADANIGREEN", "ADANITRANS",
    "VEDL", "SHREECEM", "BAJAJ-AUTO", "HEROMOTOCO", "WIPRO", "TECHM", "COALINDIA", "BPCL", "GAIL",
    "IOC", "UPL", "EICHERMOT"
}

# ============ HELPERS ============
def get_last_date_from_excel(path):
    xls = pd.ExcelFile(path)
    dates = []
    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet)
            if 'DATE' in df.columns:
                dates.append(df['DATE'].max())
        except Exception:
            continue
    return max(dates) if dates else datetime(2020, 1, 1)

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def download_new_bhavcopies(start_date, save_dir):
    os.makedirs(save_dir, exist_ok=True)
    base_url = "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.nseindia.com"
    }
    for date in daterange(start_date, datetime.today()):
        if date.weekday() >= 5:  # Skip weekends
            continue
        date_str = date.strftime("%Y%m%d")
        file_name = f"BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"
        zip_path = os.path.join(save_dir, file_name)
        if os.path.exists(zip_path):
            continue
        try:
            response = requests.get(base_url.format(date=date_str), headers=headers, timeout=20)
            if response.status_code == 200 and "zip" in response.headers.get("Content-Type", ""):
                with open(zip_path, "wb") as f:
                    f.write(response.content)
                print(f"[DOWNLOADED] {file_name}")
        except Exception as e:
            print(f"[ERROR] Failed to download {file_name}: {e}")

def extract_and_process_zips(zip_dir):
    combined = []
    for file in sorted(os.listdir(zip_dir)):
        if file.endswith(".zip"):
            date_str = file.split("_")[6]
            trade_date = datetime.strptime(date_str, "%Y%m%d")
            try:
                with zipfile.ZipFile(os.path.join(zip_dir, file), 'r') as z:
                    for csv_file in z.namelist():
                        with z.open(csv_file) as f:
                            df = pd.read_csv(f)
                            df['DATE'] = trade_date
                            df = df[df['TckrSymb'].isin(NIFTY_50_SYMBOLS)].copy()
                            combined.append(df)
            except Exception as e:
                print(f"[ERROR] Processing {file}: {e}")
    return pd.concat(combined, ignore_index=True) if combined else pd.DataFrame()

def perform_pcr_feature_engineering(df):
    df.rename(columns={'TckrSymb': 'COMPANY', 'OptnTp': 'OPTION_TYPE', 'XpryDt': 'EXPIRY_DATE', 'TtlTradgVol': 'TOTAL_TRADING_VOLUME'}, inplace=True)
    df = df[['DATE', 'COMPANY', 'OPTION_TYPE', 'EXPIRY_DATE', 'TOTAL_TRADING_VOLUME']]
    
    final_dict = {}
    for company in df['COMPANY'].unique():
        sub_df = df[df['COMPANY'] == company]
        pe = sub_df[sub_df['OPTION_TYPE'] == 'PE'].groupby(['DATE', 'EXPIRY_DATE'])['TOTAL_TRADING_VOLUME'].sum()
        ce = sub_df[sub_df['OPTION_TYPE'] == 'CE'].groupby(['DATE', 'EXPIRY_DATE'])['TOTAL_TRADING_VOLUME'].sum()

        merged = pd.merge(pe, ce, on=['DATE', 'EXPIRY_DATE'], how='inner', suffixes=('_PE', '_CE')).reset_index()
        merged['COMPANY'] = company
        merged['PUT_CONTRACTS'] = merged['TOTAL_TRADING_VOLUME_PE']
        merged['CALL_CONTRACTS'] = merged['TOTAL_TRADING_VOLUME_CE']
        merged['CALL_CONTRACTS'].replace(0, 0.000001, inplace=True)
        merged['PCR_RATIO'] = (merged['PUT_CONTRACTS'] / merged['CALL_CONTRACTS']).round(4)

        merged.sort_values('DATE', inplace=True)
        merged['PCR_5DAY_AVG'] = merged['PCR_RATIO'].rolling(5).mean().round(4)
        merged['PCR_VS_5DAY_AVG'] = (merged['PCR_RATIO'] - merged['PCR_5DAY_AVG']).round(4)

        mean, std = merged['PCR_RATIO'].mean(), merged['PCR_RATIO'].std()
        merged['PCR_ZSCORE'] = ((merged['PCR_RATIO'] - mean) / std).round(4)
        merged['PCR_SPIKE_DIP_SIGNAL'] = merged['PCR_ZSCORE'].apply(lambda z: "Spike" if z > 1 else "Dip" if z < -1 else "Normal")
        merged['PCR_deviation'] = (merged['PCR_RATIO'] - merged['PCR_5DAY_AVG']).abs().round(4)

        def flag(row):
            if pd.isna(row['PCR_5DAY_AVG']):
                return "0"
            if row['PCR_RATIO'] > row['PCR_5DAY_AVG'] + 0.2:
                return "1"
            elif row['PCR_RATIO'] < row['PCR_5DAY_AVG'] - 0.2:
                return "-1"
            return "0"

        merged['PCR_flag'] = merged.apply(flag, axis=1)

        def label(row):
                if row['PCR_RATIO'] > 1.2 and row['PCR_flag'] == "HIGH":
                    return "sell"
                elif row['PCR_RATIO'] < 0.8 and row['PCR_flag'] == "LOW":
                    return "buy"
                else:
                    return "hold"

        merged['label'] = merged.apply(label, axis=1)

        final_cols = [
            'DATE', 'EXPIRY_DATE', 'COMPANY', 'PUT_CONTRACTS', 'CALL_CONTRACTS', 'PCR_RATIO',
            'PCR_5DAY_AVG', 'PCR_VS_5DAY_AVG', 'PCR_ZSCORE', 'PCR_SPIKE_DIP_SIGNAL',
            'PCR_deviation', 'PCR_flag', 'label'
        ]
        merged = merged[final_cols]

        final_dict[company] = merged
    return final_dict

def write_to_excel_append(path, pcr_dict):
    with pd.ExcelFile(path) as xls, pd.ExcelWriter(path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        for company, new_df in pcr_dict.items():
            try:
                # Try to read the old data
                old_df = pd.read_excel(xls, sheet_name=company)
                # Combine old and new
                combined_df = pd.concat([old_df, new_df], ignore_index=True)
                # Drop duplicates based on key columns (adjust as needed)
                combined_df.drop_duplicates(subset=['DATE', 'EXPIRY_DATE'], keep='last', inplace=True)
            except Exception:
                # If sheet doesn't exist, just use new data
                combined_df = new_df
            # Write back to the sheet
            combined_df.to_excel(writer, sheet_name=company, index=False)

# ============ MAIN PIPELINE ============
if __name__ == "__main__":
    last_date = get_last_date_from_excel(EXISTING_XLSX_PATH)
    start_date = last_date + timedelta(days=1)
    
    print(f"🕒 Last available date: {last_date.strftime('%Y-%m-%d')}")
    print(f"⬇️  Downloading new bhavcopies from {start_date.strftime('%Y-%m-%d')} to today...")
    download_new_bhavcopies(start_date, TEMP_DIR)

    print("🧹 Extracting and processing bhavcopies...")
    df_new = extract_and_process_zips(TEMP_DIR)

    if df_new.empty:
        print("⚠️ No new data found. Exiting.")
    else:
        print("📈 Performing PCR feature engineering...")
        pcr_result = perform_pcr_feature_engineering(df_new)

        print("💾 Appending processed data into original Excel file...")
        write_to_excel_append(EXISTING_XLSX_PATH, pcr_result)

        print("✅ Update complete.")
