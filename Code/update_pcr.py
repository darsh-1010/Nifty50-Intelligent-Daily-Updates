# import os
# import requests
# import zipfile
# import pandas as pd
# import numpy as np
# from datetime import datetime, timedelta
# from openpyxl import load_workbook

# # ============ USER INPUT ============
# EXISTING_XLSX_PATH = r"Databases/Nifty_50_PCR_Hisotrical_Data.xlsx"  # <-- PUT YOUR ACTUAL PATH HERE
# TEMP_DIR = "temp_udiff_downloads"
# PROCESSED_XLSX_PATH = "Updated_PCR_Output.xlsx"  # Final PCR output (can be same as input if overwrite allowed)

# NIFTY_50_SYMBOLS = {
#     "RELIANCE", "TCS", "HDFC", "INFY", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "BHARTIARTL", "ITC", "LT",
#     "ASIANPAINT", "HINDUNILVR", "MARUTI", "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "SBIN", "NTPC",
#     "POWERGRID", "ULTRACEMCO", "NESTLEIND", "BRITANNIA", "M&M", "SUNPHARMA", "DIVISLAB", "INDUSINDBK",
#     "TATAMOTORS", "TITAN", "DRREDDY", "GRASIM", "ADANIPORTS", "ADANIENT", "ADANIGREEN", "ADANITRANS",
#     "VEDL", "SHREECEM", "BAJAJ-AUTO", "HEROMOTOCO", "WIPRO", "TECHM", "COALINDIA", "BPCL", "GAIL",
#     "IOC", "UPL", "EICHERMOT"
# }

# # ============ HELPERS ============
# def get_last_date_from_excel(path):
#     xls = pd.ExcelFile(path)
#     dates = []
#     for sheet in xls.sheet_names:
#         try:
#             df = pd.read_excel(xls, sheet_name=sheet)
#             if 'DATE' in df.columns:
#                 dates.append(df['DATE'].max())
#         except Exception:
#             continue
#     return max(dates) if dates else datetime(2020, 1, 1)

# def daterange(start_date, end_date):
#     for n in range((end_date - start_date).days + 1):
#         yield start_date + timedelta(n)

# def download_new_bhavcopies(start_date, save_dir):
#     os.makedirs(save_dir, exist_ok=True)
#     base_url = "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip"
#     headers = {
#         "User-Agent": "Mozilla/5.0",
#         "Referer": "https://www.nseindia.com"
#     }
#     for date in daterange(start_date, datetime.today()):
#         if date.weekday() >= 5:  # Skip weekends
#             continue
#         date_str = date.strftime("%Y%m%d")
#         file_name = f"BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"
#         zip_path = os.path.join(save_dir, file_name)
#         if os.path.exists(zip_path):
#             continue
#         try:
#             response = requests.get(base_url.format(date=date_str), headers=headers, timeout=20)
#             if response.status_code == 200 and "zip" in response.headers.get("Content-Type", ""):
#                 with open(zip_path, "wb") as f:
#                     f.write(response.content)
#                 print(f"[DOWNLOADED] {file_name}")
#         except Exception as e:
#             print(f"[ERROR] Failed to download {file_name}: {e}")

# def extract_and_process_zips(zip_dir):
#     combined = []
#     for file in sorted(os.listdir(zip_dir)):
#         if file.endswith(".zip"):
#             date_str = file.split("_")[6]
#             trade_date = datetime.strptime(date_str, "%Y%m%d")
#             try:
#                 with zipfile.ZipFile(os.path.join(zip_dir, file), 'r') as z:
#                     for csv_file in z.namelist():
#                         with z.open(csv_file) as f:
#                             df = pd.read_csv(f)
#                             df['DATE'] = trade_date
#                             df = df[df['TckrSymb'].isin(NIFTY_50_SYMBOLS)].copy()
#                             combined.append(df)
#             except Exception as e:
#                 print(f"[ERROR] Processing {file}: {e}")
#     return pd.concat(combined, ignore_index=True) if combined else pd.DataFrame()

# def perform_pcr_feature_engineering(df):
#     df.rename(columns={'TckrSymb': 'COMPANY', 'OptnTp': 'OPTION_TYPE', 'XpryDt': 'EXPIRY_DATE', 'TtlTradgVol': 'TOTAL_TRADING_VOLUME'}, inplace=True)
#     df = df[['DATE', 'COMPANY', 'OPTION_TYPE', 'EXPIRY_DATE', 'TOTAL_TRADING_VOLUME']]
    
#     final_dict = {}
#     for company in df['COMPANY'].unique():
#         sub_df = df[df['COMPANY'] == company]
#         pe = sub_df[sub_df['OPTION_TYPE'] == 'PE'].groupby(['DATE', 'EXPIRY_DATE'])['TOTAL_TRADING_VOLUME'].sum()
#         ce = sub_df[sub_df['OPTION_TYPE'] == 'CE'].groupby(['DATE', 'EXPIRY_DATE'])['TOTAL_TRADING_VOLUME'].sum()

#         merged = pd.merge(pe, ce, on=['DATE', 'EXPIRY_DATE'], how='inner', suffixes=('_PE', '_CE')).reset_index()
#         merged['COMPANY'] = company
#         merged['PUT_CONTRACTS'] = merged['TOTAL_TRADING_VOLUME_PE']
#         merged['CALL_CONTRACTS'] = merged['TOTAL_TRADING_VOLUME_CE']
#         merged['CALL_CONTRACTS'].replace(0, 0.000001, inplace=True)
#         merged['PCR_RATIO'] = (merged['PUT_CONTRACTS'] / merged['CALL_CONTRACTS']).round(4)

#         merged.sort_values('DATE', inplace=True)
#         merged['PCR_5DAY_AVG'] = merged['PCR_RATIO'].rolling(5).mean().round(4)
#         merged['PCR_VS_5DAY_AVG'] = (merged['PCR_RATIO'] - merged['PCR_5DAY_AVG']).round(4)

#         mean, std = merged['PCR_RATIO'].mean(), merged['PCR_RATIO'].std()
#         merged['PCR_ZSCORE'] = ((merged['PCR_RATIO'] - mean) / std).round(4)
#         merged['PCR_SPIKE_DIP_SIGNAL'] = merged['PCR_ZSCORE'].apply(lambda z: "Spike" if z > 1 else "Dip" if z < -1 else "Normal")
#         merged['PCR_deviation'] = (merged['PCR_RATIO'] - merged['PCR_5DAY_AVG']).abs().round(4)

#         def flag(row):
#             if pd.isna(row['PCR_5DAY_AVG']):
#                 return "0"
#             if row['PCR_RATIO'] > row['PCR_5DAY_AVG'] + 0.2:
#                 return "1"
#             elif row['PCR_RATIO'] < row['PCR_5DAY_AVG'] - 0.2:
#                 return "-1"
#             return "0"

#         merged['PCR_flag'] = merged.apply(flag, axis=1)

#         def label(row):
#                 if row['PCR_RATIO'] > 1.2 and row['PCR_flag'] == "HIGH":
#                     return "sell"
#                 elif row['PCR_RATIO'] < 0.8 and row['PCR_flag'] == "LOW":
#                     return "buy"
#                 else:
#                     return "hold"

#         merged['label'] = merged.apply(label, axis=1)

#         final_cols = [
#             'DATE', 'EXPIRY_DATE', 'COMPANY', 'PUT_CONTRACTS', 'CALL_CONTRACTS', 'PCR_RATIO',
#             'PCR_5DAY_AVG', 'PCR_VS_5DAY_AVG', 'PCR_ZSCORE', 'PCR_SPIKE_DIP_SIGNAL',
#             'PCR_deviation', 'PCR_flag', 'label'
#         ]
#         merged = merged[final_cols]

#         final_dict[company] = merged
#     return final_dict

# def write_to_excel_append(path, pcr_dict):
#     with pd.ExcelFile(path) as xls, pd.ExcelWriter(path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
#         for company, new_df in pcr_dict.items():
#             try:
#                 # Try to read the old data
#                 old_df = pd.read_excel(xls, sheet_name=company)
#                 # Combine old and new
#                 combined_df = pd.concat([old_df, new_df], ignore_index=True)
#                 # Drop duplicates based on key columns (adjust as needed)
#                 combined_df.drop_duplicates(subset=['DATE', 'EXPIRY_DATE'], keep='last', inplace=True)
#             except Exception:
#                 # If sheet doesn't exist, just use new data
#                 combined_df = new_df
#             # Write back to the sheet
#             combined_df.to_excel(writer, sheet_name=company, index=False)

# # ============ MAIN PIPELINE ============
# if __name__ == "__main__":
#     last_date = get_last_date_from_excel(EXISTING_XLSX_PATH)
#     start_date = last_date + timedelta(days=1)
    
#     print(f"🕒 Last available date: {last_date.strftime('%Y-%m-%d')}")
#     print(f"⬇️  Downloading new bhavcopies from {start_date.strftime('%Y-%m-%d')} to today...")
#     download_new_bhavcopies(start_date, TEMP_DIR)

#     print("🧹 Extracting and processing bhavcopies...")
#     df_new = extract_and_process_zips(TEMP_DIR)

#     if df_new.empty:
#         print("⚠️ No new data found. Exiting.")
#     else:
#         print("📈 Performing PCR feature engineering...")
#         pcr_result = perform_pcr_feature_engineering(df_new)

#         print("💾 Appending processed data into original Excel file...")
#         write_to_excel_append(EXISTING_XLSX_PATH, pcr_result)

#         print("✅ Update complete.")





# import requests
# import pandas as pd
# from datetime import datetime
# import time

# historical_file = r"Databases/Nifty_50_PCR_Hisotrical_Data.xlsx"

# def create_nse_session():
#     session = requests.Session()
#     headers = {
#         "User-Agent": "Mozilla/5.0",
#         "Accept-Language": "en-US,en;q=0.9",
#         "Accept": "*/*",
#         "Referer": "https://www.nseindia.com/",
#         "Connection": "keep-alive"
#     }
#     session.headers.update(headers)
#     try:
#         session.get("https://www.nseindia.com/option-chain", timeout=5)
#     except Exception as e:
#         print("⚠️ Error establishing session:", e)
#     return session

# def compute_pcr_flag(row):
#     if pd.isna(row['PCR_5DAY_AVG']):
#         return "0"
#     if row['PCR_RATIO'] > row['PCR_5DAY_AVG'] + 0.2:
#         return "1"
#     elif row['PCR_RATIO'] < row['PCR_5DAY_AVG'] - 0.2:
#         return "-1"
#     return "0"

# def compute_label(row):
#     if row['PCR_RATIO'] > 1.2 and row['PCR_flag'] == "1":
#         return "sell"
#     elif row['PCR_RATIO'] < 0.8 and row['PCR_flag'] == "-1":
#         return "buy"
#     else:
#         return "hold"

# def fetch_option_chain_data(session, symbol):
#     url = f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"
#     try:
#         response = session.get(url, timeout=10)
#         response.raise_for_status()
#         return response.json()
#     except Exception as e:
#         print(f"❌ Error fetching data for {symbol}: {e}")
#         return None

# def analyze_oi_data(data, symbol):
#     records = data.get("records", {}).get("data", [])
#     expiry = data.get("records", {}).get("expiryDates", ["N/A"])[0]
#     # Use symbol as company name to avoid UNKNOWN
#     company = symbol

#     # try:
#     #     expiry = datetime.strptime(expiry, "%d-%b-%Y").strftime("%Y-%m-%d")
#     # except Exception:
#     #     expiry = expiry  # fallback if format doesn't match

#     pe_oi, ce_oi, pe_vol, ce_vol = 0, 0, 0, 0
#     for item in records:
#         if item.get("expiryDate") != expiry:
#             continue
#         if "CE" in item:
#             ce = item["CE"]
#             ce_oi += ce.get("openInterest", 0)
#             ce_vol += ce.get("totalTradedVolume", 0)
#         if "PE" in item:
#             pe = item["PE"]
#             pe_oi += pe.get("openInterest", 0)
#             pe_vol += pe.get("totalTradedVolume", 0)

#     if ce_oi == 0:
#         ce_oi = 1e-8

#     pcr = round(pe_oi / ce_oi, 2) if ce_oi else 0

#     return {
#         "DATE": datetime.now().strftime("%Y-%m-%d"),
#         "COMPANY": company,
#         # Removed SYMBOL key here
#         "PUT_CONTRACTS": pe_vol,
#         "CALL_CONTRACTS": ce_vol,
#         "PCR_RATIO": pcr,
#         "EXPIRY_DATE": expiry
#     }

# def generate_live_data(symbols):
#     session = create_nse_session()
#     results = []
#     for symbol in symbols:
#         print(f"Fetching live data for {symbol}...")
#         data = fetch_option_chain_data(session, symbol)
#         if data:
#             result = analyze_oi_data(data, symbol)
#             if result:
#                 results.append(result)
#             else:
#                 print(f"Skipped {symbol} due to incomplete data.")
#         else:
#             print(f"No data for {symbol}")
#         time.sleep(1.5)
#     return pd.DataFrame(results)

# def update_existing_excel(historical_path, live_df):
#     today = pd.to_datetime(datetime.now().date())
#     historical_data = pd.read_excel(historical_path, sheet_name=None)

#     # Update sheets in memory
#     for company in live_df["COMPANY"].unique():
#         if company not in historical_data:
#             print(f"Skipping {company}, not in historical file")
#             continue

#         hist_df = historical_data[company]
#         hist_df["DATE"] = pd.to_datetime(hist_df["DATE"])

#         if today in hist_df["DATE"].values:
#             print(f"{company} already updated for today, skipping")
#             continue

#         new_row = live_df[live_df["COMPANY"] == company].copy()
#         new_row["DATE"] = pd.to_datetime(new_row["DATE"])

#         # Append new row and sort
#         updated_df = pd.concat([hist_df, new_row], ignore_index=True).sort_values("DATE")

#         # Calculate rolling features
#         updated_df["PCR_5DAY_AVG"] = updated_df["PCR_RATIO"].rolling(window=5).mean()
#         updated_df["PCR_VS_5DAY_AVG"] = (updated_df["PCR_RATIO"] - updated_df["PCR_5DAY_AVG"]).round(4)
#         updated_df["PCR_deviation"] = (updated_df["PCR_RATIO"] - updated_df["PCR_5DAY_AVG"]).abs().round(4)
#         updated_df["PCR_ZSCORE"] = (
#             (updated_df["PCR_RATIO"] - updated_df["PCR_5DAY_AVG"]) /
#             updated_df["PCR_RATIO"].rolling(5).std()
#         )
#         updated_df["PCR_SPIKE_DIP_SIGNAL"] = updated_df["PCR_ZSCORE"].apply(
#             lambda z: "Spike" if z > 2 else "Dip" if z < -2 else "Normal"
#         )

#         # Compute PCR_flag
#         updated_df['PCR_flag'] = updated_df.apply(compute_pcr_flag, axis=1)

#         # Compute label based on PCR_flag and PCR_RATIO
#         updated_df['label'] = updated_df.apply(compute_label, axis=1)


#         historical_data[company] = updated_df

#     # Write back all sheets to Excel
#     with pd.ExcelWriter(historical_path, engine="openpyxl", mode='w') as writer:
#         for sheet_name, df in historical_data.items():
#             df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

#     print(f"✅ Historical Excel updated at {historical_path}")

# if __name__ == "__main__":
#     symbols = [
#         "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "BHARTIARTL", "ITC", "LT",
#         "ASIANPAINT", "HINDUNILVR", "MARUTI", "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "SBIN", "NTPC",
#         "POWERGRID", "ULTRACEMCO", "NESTLEIND", "BRITANNIA", "M&M", "SUNPHARMA", "DIVISLAB", "INDUSINDBK",
#         "TATAMOTORS", "TITAN", "DRREDDY", "GRASIM", "ADANIPORTS", "ADANIENT", "ADANIGREEN", 
#         "VEDL", "SHREECEM", "BAJAJ-AUTO", "HEROMOTOCO", "WIPRO", "TECHM", "COALINDIA", "BPCL", "GAIL",
#         "IOC", "UPL", "EICHERMOT"
#     ]

#     live_df = generate_live_data(symbols)
#     if not live_df.empty:
#         update_existing_excel(historical_file, live_df)
#     else:
#         print("No live data fetched")

#============================================================================================================================TRY3============================================================================================================

import pandas as pd
from datetime import datetime
import time
from jugaad_data.nse import NSEOptionChain

historical_file = r"Databases/Nifty_50_PCR_Hisotrical_Data.xlsx"

def compute_pcr_flag(row):
    if pd.isna(row['PCR_5DAY_AVG']):
        return "0"
    if row['PCR_RATIO'] > row['PCR_5DAY_AVG'] + 0.2:
        return "1"
    elif row['PCR_RATIO'] < row['PCR_5DAY_AVG'] - 0.2:
        return "-1"
    return "0"

def compute_label(row):
    if row['PCR_RATIO'] > 1.2 and row['PCR_flag'] == "1":
        return "sell"
    elif row['PCR_RATIO'] < 0.8 and row['PCR_flag'] == "-1":
        return "buy"
    else:
        return "hold"

def fetch_option_chain_jugaad(symbol):
    try:
        data = NSEOptionChain(symbol).get_option_chain_data()
        return data
    except Exception as e:
        print(f"❌ Error fetching data for {symbol}: {e}")
        return None

def analyze_oi_data(data, symbol):
    # Get nearest expiry
    expiry_dates = sorted(set(data['expiryDate']))
    if not expiry_dates:
        return None
    expiry = expiry_dates[0]

    pe_oi = ce_oi = pe_vol = ce_vol = 0

    for _, row in data.iterrows():
        if row['expiryDate'] != expiry:
            continue
        if not pd.isna(row.get('PE OI')):
            pe_oi += int(row.get('PE OI', 0))
            pe_vol += int(row.get('PE Volume', 0))
        if not pd.isna(row.get('CE OI')):
            ce_oi += int(row.get('CE OI', 0))
            ce_vol += int(row.get('CE Volume', 0))

    if ce_oi == 0:
        ce_oi = 1e-8

    pcr = round(pe_oi / ce_oi, 2)

    return {
        "DATE": datetime.now().strftime("%Y-%m-%d"),
        "COMPANY": symbol,
        "PUT_CONTRACTS": pe_vol,
        "CALL_CONTRACTS": ce_vol,
        "PCR_RATIO": pcr,
        "EXPIRY_DATE": expiry
    }

def generate_live_data(symbols):
    results = []
    for symbol in symbols:
        print(f"🔄 Fetching live data for {symbol}...")
        data = fetch_option_chain_jugaad(symbol)
        if data is not None and not data.empty:
            result = analyze_oi_data(data, symbol)
            if result:
                results.append(result)
            else:
                print(f"⚠️ Skipped {symbol} due to incomplete data.")
        else:
            print(f"❌ No data for {symbol}")
        time.sleep(1.5)  # to avoid rate-limiting
    return pd.DataFrame(results)

def update_existing_excel(historical_path, live_df):
    today = pd.to_datetime(datetime.now().date())
    historical_data = pd.read_excel(historical_path, sheet_name=None)

    for company in live_df["COMPANY"].unique():
        if company not in historical_data:
            print(f"⚠️ Skipping {company}, not in historical file")
            continue

        hist_df = historical_data[company]
        hist_df["DATE"] = pd.to_datetime(hist_df["DATE"])

        if today in hist_df["DATE"].values:
            print(f"✅ {company} already updated for today, skipping")
            continue

        new_row = live_df[live_df["COMPANY"] == company].copy()
        new_row["DATE"] = pd.to_datetime(new_row["DATE"])

        updated_df = pd.concat([hist_df, new_row], ignore_index=True).sort_values("DATE")

        updated_df["PCR_5DAY_AVG"] = updated_df["PCR_RATIO"].rolling(window=5).mean()
        updated_df["PCR_VS_5DAY_AVG"] = (updated_df["PCR_RATIO"] - updated_df["PCR_5DAY_AVG"]).round(4)
        updated_df["PCR_deviation"] = (updated_df["PCR_RATIO"] - updated_df["PCR_5DAY_AVG"]).abs().round(4)
        updated_df["PCR_ZSCORE"] = (
            (updated_df["PCR_RATIO"] - updated_df["PCR_5DAY_AVG"]) /
            updated_df["PCR_RATIO"].rolling(5).std()
        )
        updated_df["PCR_SPIKE_DIP_SIGNAL"] = updated_df["PCR_ZSCORE"].apply(
            lambda z: "Spike" if z > 2 else "Dip" if z < -2 else "Normal"
        )

        updated_df['PCR_flag'] = updated_df.apply(compute_pcr_flag, axis=1)
        updated_df['label'] = updated_df.apply(compute_label, axis=1)

        historical_data[company] = updated_df

    with pd.ExcelWriter(historical_path, engine="openpyxl", mode='w') as writer:
        for sheet_name, df in historical_data.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

    print(f"✅ Historical Excel updated at {historical_path}")

if __name__ == "__main__":
    symbols = [
        "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "BHARTIARTL", "ITC", "LT",
        "ASIANPAINT", "HINDUNILVR", "MARUTI", "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "SBIN", "NTPC",
        "POWERGRID", "ULTRACEMCO", "NESTLEIND", "BRITANNIA", "M&M", "SUNPHARMA", "DIVISLAB", "INDUSINDBK",
        "TATAMOTORS", "TITAN", "DRREDDY", "GRASIM", "ADANIPORTS", "ADANIENT", "ADANIGREEN", 
        "VEDL", "SHREECEM", "BAJAJ-AUTO", "HEROMOTOCO", "WIPRO", "TECHM", "COALINDIA", "BPCL", "GAIL",
        "IOC", "UPL", "EICHERMOT"
    ]

    live_df = generate_live_data(symbols)
    if not live_df.empty:
        update_existing_excel(historical_file, live_df)
    else:
        print("❌ No live data fetched.")
