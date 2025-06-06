
# # import os
# # import requests
# # import zipfile
# # import pandas as pd
# # import numpy as np
# # from datetime import datetime, timedelta
# # from openpyxl import load_workbook

# # # ============ USER INPUT ============
# # EXISTING_XLSX_PATH = r"Databases/Nifty_50_PCR_Hisotrical_Data.xlsx"  # <-- PUT YOUR ACTUAL PATH HERE
# # TEMP_DIR = "temp_udiff_downloads"
# # PROCESSED_XLSX_PATH = "Updated_PCR_Output.xlsx"  # Final PCR output (can be same as input if overwrite allowed)

# # NIFTY_50_SYMBOLS = {
# #     "RELIANCE", "TCS", "HDFC", "INFY", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "BHARTIARTL", "ITC", "LT",
# #     "ASIANPAINT", "HINDUNILVR", "MARUTI", "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "SBIN", "NTPC",
# #     "POWERGRID", "ULTRACEMCO", "NESTLEIND", "BRITANNIA", "M&M", "SUNPHARMA", "DIVISLAB", "INDUSINDBK",
# #     "TATAMOTORS", "TITAN", "DRREDDY", "GRASIM", "ADANIPORTS", "ADANIENT", "ADANIGREEN", "ADANITRANS",
# #     "VEDL", "SHREECEM", "BAJAJ-AUTO", "HEROMOTOCO", "WIPRO", "TECHM", "COALINDIA", "BPCL", "GAIL",
# #     "IOC", "UPL", "EICHERMOT"
# # }

# # # ============ HELPERS ============
# # def get_last_date_from_excel(path):
# #     xls = pd.ExcelFile(path)
# #     dates = []
# #     for sheet in xls.sheet_names:
# #         try:
# #             df = pd.read_excel(xls, sheet_name=sheet)
# #             if 'DATE' in df.columns:
# #                 dates.append(df['DATE'].max())
# #         except Exception:
# #             continue
# #     return max(dates) if dates else datetime(2020, 1, 1)

# # def daterange(start_date, end_date):
# #     for n in range((end_date - start_date).days + 1):
# #         yield start_date + timedelta(n)

# # def download_new_bhavcopies(start_date, save_dir):
# #     os.makedirs(save_dir, exist_ok=True)
# #     base_url = "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip"
# #     headers = {
# #         "User-Agent": "Mozilla/5.0",
# #         "Referer": "https://www.nseindia.com"
# #     }
# #     for date in daterange(start_date, datetime.today()):
# #         if date.weekday() >= 5:  # Skip weekends
# #             continue
# #         date_str = date.strftime("%Y%m%d")
# #         file_name = f"BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"
# #         zip_path = os.path.join(save_dir, file_name)
# #         if os.path.exists(zip_path):
# #             continue
# #         try:
# #             response = requests.get(base_url.format(date=date_str), headers=headers, timeout=20)
# #             if response.status_code == 200 and "zip" in response.headers.get("Content-Type", ""):
# #                 with open(zip_path, "wb") as f:
# #                     f.write(response.content)
# #                 print(f"[DOWNLOADED] {file_name}")
# #         except Exception as e:
# #             print(f"[ERROR] Failed to download {file_name}: {e}")

# # def extract_and_process_zips(zip_dir):
# #     combined = []
# #     for file in sorted(os.listdir(zip_dir)):
# #         if file.endswith(".zip"):
# #             date_str = file.split("_")[6]
# #             trade_date = datetime.strptime(date_str, "%Y%m%d")
# #             try:
# #                 with zipfile.ZipFile(os.path.join(zip_dir, file), 'r') as z:
# #                     for csv_file in z.namelist():
# #                         with z.open(csv_file) as f:
# #                             df = pd.read_csv(f)
# #                             df['DATE'] = trade_date
# #                             df = df[df['TckrSymb'].isin(NIFTY_50_SYMBOLS)].copy()
# #                             combined.append(df)
# #             except Exception as e:
# #                 print(f"[ERROR] Processing {file}: {e}")
# #     return pd.concat(combined, ignore_index=True) if combined else pd.DataFrame()

# # def perform_pcr_feature_engineering(df):
# #     df.rename(columns={'TckrSymb': 'COMPANY', 'OptnTp': 'OPTION_TYPE', 'XpryDt': 'EXPIRY_DATE', 'TtlTradgVol': 'TOTAL_TRADING_VOLUME'}, inplace=True)
# #     df = df[['DATE', 'COMPANY', 'OPTION_TYPE', 'EXPIRY_DATE', 'TOTAL_TRADING_VOLUME']]
    
# #     final_dict = {}
# #     for company in df['COMPANY'].unique():
# #         sub_df = df[df['COMPANY'] == company]
# #         pe = sub_df[sub_df['OPTION_TYPE'] == 'PE'].groupby(['DATE', 'EXPIRY_DATE'])['TOTAL_TRADING_VOLUME'].sum()
# #         ce = sub_df[sub_df['OPTION_TYPE'] == 'CE'].groupby(['DATE', 'EXPIRY_DATE'])['TOTAL_TRADING_VOLUME'].sum()

# #         merged = pd.merge(pe, ce, on=['DATE', 'EXPIRY_DATE'], how='inner', suffixes=('_PE', '_CE')).reset_index()
# #         merged['COMPANY'] = company
# #         merged['PUT_CONTRACTS'] = merged['TOTAL_TRADING_VOLUME_PE']
# #         merged['CALL_CONTRACTS'] = merged['TOTAL_TRADING_VOLUME_CE']
# #         merged['CALL_CONTRACTS'].replace(0, 0.000001, inplace=True)
# #         merged['PCR_RATIO'] = (merged['PUT_CONTRACTS'] / merged['CALL_CONTRACTS']).round(4)

# #         merged.sort_values('DATE', inplace=True)
# #         merged['PCR_5DAY_AVG'] = merged['PCR_RATIO'].rolling(5).mean().round(4)
# #         merged['PCR_VS_5DAY_AVG'] = (merged['PCR_RATIO'] - merged['PCR_5DAY_AVG']).round(4)

# #         mean, std = merged['PCR_RATIO'].mean(), merged['PCR_RATIO'].std()
# #         merged['PCR_ZSCORE'] = ((merged['PCR_RATIO'] - mean) / std).round(4)
# #         merged['PCR_SPIKE_DIP_SIGNAL'] = merged['PCR_ZSCORE'].apply(lambda z: "Spike" if z > 1 else "Dip" if z < -1 else "Normal")
# #         merged['PCR_deviation'] = (merged['PCR_RATIO'] - merged['PCR_5DAY_AVG']).abs().round(4)

# #         def flag(row):
# #             if pd.isna(row['PCR_5DAY_AVG']):
# #                 return "0"
# #             if row['PCR_RATIO'] > row['PCR_5DAY_AVG'] + 0.2:
# #                 return "1"
# #             elif row['PCR_RATIO'] < row['PCR_5DAY_AVG'] - 0.2:
# #                 return "-1"
# #             return "0"

# #         merged['PCR_flag'] = merged.apply(flag, axis=1)

# #         def label(row):
# #                 if row['PCR_RATIO'] > 1.2 and row['PCR_flag'] == "HIGH":
# #                     return "sell"
# #                 elif row['PCR_RATIO'] < 0.8 and row['PCR_flag'] == "LOW":
# #                     return "buy"
# #                 else:
# #                     return "hold"

# #         merged['label'] = merged.apply(label, axis=1)

# #         final_cols = [
# #             'DATE', 'EXPIRY_DATE', 'COMPANY', 'PUT_CONTRACTS', 'CALL_CONTRACTS', 'PCR_RATIO',
# #             'PCR_5DAY_AVG', 'PCR_VS_5DAY_AVG', 'PCR_ZSCORE', 'PCR_SPIKE_DIP_SIGNAL',
# #             'PCR_deviation', 'PCR_flag', 'label'
# #         ]
# #         merged = merged[final_cols]

# #         final_dict[company] = merged
# #     return final_dict

# # def write_to_excel_append(path, pcr_dict):
# #     with pd.ExcelFile(path) as xls, pd.ExcelWriter(path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
# #         for company, new_df in pcr_dict.items():
# #             try:
# #                 # Try to read the old data
# #                 old_df = pd.read_excel(xls, sheet_name=company)
# #                 # Combine old and new
# #                 combined_df = pd.concat([old_df, new_df], ignore_index=True)
# #                 # Drop duplicates based on key columns (adjust as needed)
# #                 combined_df.drop_duplicates(subset=['DATE', 'EXPIRY_DATE'], keep='last', inplace=True)
# #             except Exception:
# #                 # If sheet doesn't exist, just use new data
# #                 combined_df = new_df
# #             # Write back to the sheet
# #             combined_df.to_excel(writer, sheet_name=company, index=False)

# # # ============ MAIN PIPELINE ============
# # if __name__ == "__main__":
# #     last_date = get_last_date_from_excel(EXISTING_XLSX_PATH)
# #     start_date = last_date + timedelta(days=1)
    
# #     print(f"🕒 Last available date: {last_date.strftime('%Y-%m-%d')}")
# #     print(f"⬇️  Downloading new bhavcopies from {start_date.strftime('%Y-%m-%d')} to today...")
# #     download_new_bhavcopies(start_date, TEMP_DIR)

# #     print("🧹 Extracting and processing bhavcopies...")
# #     df_new = extract_and_process_zips(TEMP_DIR)

# #     if df_new.empty:
# #         print("⚠️ No new data found. Exiting.")
# #     else:
# #         print("📈 Performing PCR feature engineering...")
# #         pcr_result = perform_pcr_feature_engineering(df_new)

# #         print("💾 Appending processed data into original Excel file...")
# #         write_to_excel_append(EXISTING_XLSX_PATH, pcr_result)

# #         print("✅ Update complete.")





# # import requests
# # import pandas as pd
# # from datetime import datetime
# # import time

# # historical_file = r"Databases/Nifty_50_PCR_Hisotrical_Data.xlsx"

# # def create_nse_session():
# #     session = requests.Session()
# #     headers = {
# #         "User-Agent": "Mozilla/5.0",
# #         "Accept-Language": "en-US,en;q=0.9",
# #         "Accept": "*/*",
# #         "Referer": "https://www.nseindia.com/",
# #         "Connection": "keep-alive"
# #     }
# #     session.headers.update(headers)
# #     try:
# #         session.get("https://www.nseindia.com/option-chain", timeout=5)
# #     except Exception as e:
# #         print("⚠️ Error establishing session:", e)
# #     return session

# # def compute_pcr_flag(row):
# #     if pd.isna(row['PCR_5DAY_AVG']):
# #         return "0"
# #     if row['PCR_RATIO'] > row['PCR_5DAY_AVG'] + 0.2:
# #         return "1"
# #     elif row['PCR_RATIO'] < row['PCR_5DAY_AVG'] - 0.2:
# #         return "-1"
# #     return "0"

# # def compute_label(row):
# #     if row['PCR_RATIO'] > 1.2 and row['PCR_flag'] == "1":
# #         return "sell"
# #     elif row['PCR_RATIO'] < 0.8 and row['PCR_flag'] == "-1":
# #         return "buy"
# #     else:
# #         return "hold"

# # def fetch_option_chain_data(session, symbol):
# #     url = f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"
# #     try:
# #         response = session.get(url, timeout=10)
# #         response.raise_for_status()
# #         return response.json()
# #     except Exception as e:
# #         print(f"❌ Error fetching data for {symbol}: {e}")
# #         return None

# # def analyze_oi_data(data, symbol):
# #     records = data.get("records", {}).get("data", [])
# #     expiry = data.get("records", {}).get("expiryDates", ["N/A"])[0]
# #     # Use symbol as company name to avoid UNKNOWN
# #     company = symbol

# #     # try:
# #     #     expiry = datetime.strptime(expiry, "%d-%b-%Y").strftime("%Y-%m-%d")
# #     # except Exception:
# #     #     expiry = expiry  # fallback if format doesn't match

# #     pe_oi, ce_oi, pe_vol, ce_vol = 0, 0, 0, 0
# #     for item in records:
# #         if item.get("expiryDate") != expiry:
# #             continue
# #         if "CE" in item:
# #             ce = item["CE"]
# #             ce_oi += ce.get("openInterest", 0)
# #             ce_vol += ce.get("totalTradedVolume", 0)
# #         if "PE" in item:
# #             pe = item["PE"]
# #             pe_oi += pe.get("openInterest", 0)
# #             pe_vol += pe.get("totalTradedVolume", 0)

# #     if ce_oi == 0:
# #         ce_oi = 1e-8

# #     pcr = round(pe_oi / ce_oi, 2) if ce_oi else 0

# #     return {
# #         "DATE": datetime.now().strftime("%Y-%m-%d"),
# #         "COMPANY": company,
# #         # Removed SYMBOL key here
# #         "PUT_CONTRACTS": pe_vol,
# #         "CALL_CONTRACTS": ce_vol,
# #         "PCR_RATIO": pcr,
# #         "EXPIRY_DATE": expiry
# #     }

# # def generate_live_data(symbols):
# #     session = create_nse_session()
# #     results = []
# #     for symbol in symbols:
# #         print(f"Fetching live data for {symbol}...")
# #         data = fetch_option_chain_data(session, symbol)
# #         if data:
# #             result = analyze_oi_data(data, symbol)
# #             if result:
# #                 results.append(result)
# #             else:
# #                 print(f"Skipped {symbol} due to incomplete data.")
# #         else:
# #             print(f"No data for {symbol}")
# #         time.sleep(1.5)
# #     return pd.DataFrame(results)

# # def update_existing_excel(historical_path, live_df):
# #     today = pd.to_datetime(datetime.now().date())
# #     historical_data = pd.read_excel(historical_path, sheet_name=None)

# #     # Update sheets in memory
# #     for company in live_df["COMPANY"].unique():
# #         if company not in historical_data:
# #             print(f"Skipping {company}, not in historical file")
# #             continue

# #         hist_df = historical_data[company]
# #         hist_df["DATE"] = pd.to_datetime(hist_df["DATE"])

# #         if today in hist_df["DATE"].values:
# #             print(f"{company} already updated for today, skipping")
# #             continue

# #         new_row = live_df[live_df["COMPANY"] == company].copy()
# #         new_row["DATE"] = pd.to_datetime(new_row["DATE"])

# #         # Append new row and sort
# #         updated_df = pd.concat([hist_df, new_row], ignore_index=True).sort_values("DATE")

# #         # Calculate rolling features
# #         updated_df["PCR_5DAY_AVG"] = updated_df["PCR_RATIO"].rolling(window=5).mean()
# #         updated_df["PCR_VS_5DAY_AVG"] = (updated_df["PCR_RATIO"] - updated_df["PCR_5DAY_AVG"]).round(4)
# #         updated_df["PCR_deviation"] = (updated_df["PCR_RATIO"] - updated_df["PCR_5DAY_AVG"]).abs().round(4)
# #         updated_df["PCR_ZSCORE"] = (
# #             (updated_df["PCR_RATIO"] - updated_df["PCR_5DAY_AVG"]) /
# #             updated_df["PCR_RATIO"].rolling(5).std()
# #         )
# #         updated_df["PCR_SPIKE_DIP_SIGNAL"] = updated_df["PCR_ZSCORE"].apply(
# #             lambda z: "Spike" if z > 2 else "Dip" if z < -2 else "Normal"
# #         )

# #         # Compute PCR_flag
# #         updated_df['PCR_flag'] = updated_df.apply(compute_pcr_flag, axis=1)

# #         # Compute label based on PCR_flag and PCR_RATIO
# #         updated_df['label'] = updated_df.apply(compute_label, axis=1)


# #         historical_data[company] = updated_df

# #     # Write back all sheets to Excel
# #     with pd.ExcelWriter(historical_path, engine="openpyxl", mode='w') as writer:
# #         for sheet_name, df in historical_data.items():
# #             df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

# #     print(f"✅ Historical Excel updated at {historical_path}")

# # if __name__ == "__main__":
# #     symbols = [
# #         "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "BHARTIARTL", "ITC", "LT",
# #         "ASIANPAINT", "HINDUNILVR", "MARUTI", "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "SBIN", "NTPC",
# #         "POWERGRID", "ULTRACEMCO", "NESTLEIND", "BRITANNIA", "M&M", "SUNPHARMA", "DIVISLAB", "INDUSINDBK",
# #         "TATAMOTORS", "TITAN", "DRREDDY", "GRASIM", "ADANIPORTS", "ADANIENT", "ADANIGREEN", 
# #         "VEDL", "SHREECEM", "BAJAJ-AUTO", "HEROMOTOCO", "WIPRO", "TECHM", "COALINDIA", "BPCL", "GAIL",
# #         "IOC", "UPL", "EICHERMOT"
# #     ]

# #     live_df = generate_live_data(symbols)
# #     if not live_df.empty:
# #         update_existing_excel(historical_file, live_df)
# #     else:
# #         print("No live data fetched")

# #============================================================================================================================TRY3============================================================================================================
# import os
# import time
# import pytz
# import numpy as np
# import pandas as pd
# from datetime import datetime
# import yfinance as yf
# import requests

# # Constants
# tz_IN = pytz.timezone('Asia/Kolkata')
# today_date = datetime.today().strftime('%Y-%m-%d')
# DB_PATH = 'Databases/Nifty_50_PCR_Hisotrical_Data.xlsx'

# # List of Nifty 50 symbols
# nifty50_symbols = [
#     "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BPCL", "BHARTIARTL", "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY",
#     "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK", "INFY", "JSWSTEEL", "KOTAKBANK", "LT", "M&M", "MARUTI", "NTPC", 
#     "NESTLEIND", "ONGC",  "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM", "TITAN", "UPL", "ULTRACEMCO", "WIPRO"
# ]

# # Fetch Option Chain Data using exact logic
# def get_option_chain(symbol):
#     datetime_IN = datetime.now(tz_IN)
#     url = f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"
#     headers = {
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
#     }
#     with requests.Session() as session:
#         session.get("https://www.nseindia.com", headers=headers , timeout=5 )
#         response = session.get(url, headers=headers , timeout =5 )

#     if response.status_code != 200:
#         print(f"Failed to fetch data for {symbol}")
#         return pd.DataFrame(), pd.DataFrame()

#     try:
#         records = response.json()['records']['data']
#     except Exception as e:
#         print(f"Error parsing data for {symbol}: {e}")
#         return pd.DataFrame(), pd.DataFrame()

#     CE_options = [
#         {"Strike Price": r["CE"]["strikePrice"], "expiryDate": r["CE"]["expiryDate"], "totalTradedVolume": r["CE"]["totalTradedVolume"]}
#         for r in records if "CE" in r
#     ]

#     PE_options = [
#         {"Strike Price": r["PE"]["strikePrice"], "expiryDate": r["PE"]["expiryDate"], "totalTradedVolume": r["PE"]["totalTradedVolume"]}
#         for r in records if "PE" in r
#     ]

#     return pd.DataFrame(CE_options), pd.DataFrame(PE_options)

# # Update and feature engineer PCR data
# def update_pcr_database():
#     for symbol in nifty50_symbols:
#         df_ce, df_pe = get_option_chain(symbol)
#         if df_ce.empty or df_pe.empty:
#             continue

#         ce_grouped = df_ce.groupby("Strike Price")["totalTradedVolume"].sum()
#         pe_grouped = df_pe.groupby("Strike Price")["totalTradedVolume"].sum()

#         df_pcr = pd.DataFrame({
#             "CALL_VOLUME": ce_grouped,
#             "PUT_VOLUME": pe_grouped
#         }).fillna(0)

#         df_pcr["PCR_RATIO"] = df_pcr["PUT_VOLUME"] / df_pcr["CALL_VOLUME"].replace(0, np.nan)
#         df_pcr.reset_index(inplace=True)
#         df_pcr["DATE"] = today_date

#         # Add expiry (first seen in CE/PE)
#         if not df_ce.empty:
#             df_pcr["EXPIRY_DATE"] = df_ce["expiryDate"].iloc[0]

#         # Add company column
#         df_pcr["COMPANY"] = symbol.upper()

#         # Load last 4 historical rows
#         sheet_name = symbol.upper()
#         if os.path.exists(DB_PATH):
#             try:
#                 historical = pd.read_excel(DB_PATH, sheet_name=sheet_name)
#             except:
#                 historical = pd.DataFrame()
#         else:
#             historical = pd.DataFrame()

#         historical = historical.sort_values("DATE")
#         last_4 = historical.tail(4)
#         combined = pd.concat([last_4, df_pcr], ignore_index=True)
#         combined.sort_values("DATE", inplace=True)

#         # Compute features
#         combined["PCR_5DAY_AVG"] = combined["PCR_RATIO"].rolling(window=5).mean()
#         combined["PCR_deviation"] = combined["PCR_RATIO"] - combined["PCR_5DAY_AVG"]
#         combined["PCR_ZSCORE"] = combined["PCR_deviation"] / combined["PCR_deviation"].rolling(window=5).std()
#         combined["PCR_SPIKE_DIP_SIGNAL"] = combined["PCR_ZSCORE"].apply(lambda x: "SPIKE" if x > 2 else ("DIP" if x < -2 else "NORMAL"))
#         combined["PCR_flag"] = combined["PCR_RATIO"].apply(lambda x: "HIGH_PCR" if x > 1.3 else ("LOW_PCR" if x < 0.7 else "NEUTRAL"))

#         # Save updated historical data
#         final_historical = pd.concat([historical, df_pcr], ignore_index=True).drop_duplicates(subset=["DATE", "Strike Price"])
#         final_historical = final_historical.merge(combined[["Strike Price", "DATE", "PCR_5DAY_AVG", "PCR_deviation", "PCR_ZSCORE", "PCR_SPIKE_DIP_SIGNAL", "PCR_flag"]], on=["Strike Price", "DATE"], how="left")

#         with pd.ExcelWriter(DB_PATH, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
#             final_historical.to_excel(writer, sheet_name=sheet_name, index=False)

#         print(f"Updated PCR data for {symbol} on {today_date}")
#         time.sleep(2)

# if __name__ == "__main__":
#     update_pcr_database()


import os
import time
import random
import numpy as np
import pandas as pd
import requests
from datetime import datetime
from openpyxl import load_workbook

# Attempt to import Selenium for cookie fallback; if not available, fallback won't work.
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
except ImportError:
    webdriver = None

# Constants
today_date = datetime.today().strftime("%Y-%m-%d")
DB_PATH = 'Databases/Nifty_50_PCR_Hisotrical_Data.xlsx'
nifty50_symbols = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV",
    "BPCL", "BHARTIARTL", "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY", "EICHERMOT", "GRASIM", "HCLTECH",
    "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK", "INFY",
    "JSWSTEEL", "KOTAKBANK", "LT", "M&M", "MARUTI", "NTPC", "NESTLEIND", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE",
    "SBIN", "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM", "TITAN", "UPL", "ULTRACEMCO", "WIPRO"
]

# List of several common user agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
]

def get_random_user_agent():
    """Returns a random User-Agent string from the list."""
    return random.choice(USER_AGENTS)

def get_cookies_via_selenium(symbol, user_agent):
    """
    Uses Selenium in headless mode to visit the NSE derivatives page for the given symbol
    and returns cookies as a dictionary.
    """
    home_url = f"https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}"
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument(f"user-agent={user_agent}")
    
    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"Selenium webdriver initialization failed for {symbol}: {e}")
        return {}
    
    try:
        driver.get(home_url)
        # Wait a few seconds to allow cookies to be set
        time.sleep(random.uniform(3, 6))
        cookies = driver.get_cookies()
    finally:
        driver.quit()
    
    cookie_dict = {}
    for cookie in cookies:
        cookie_dict[cookie["name"]] = cookie["value"]
    return cookie_dict

def fetch_option_chain(symbol):
    """
    Fetches the option chain for a given equity symbol using NSE's endpoint.
    If it receives a 403 error, it attempts a Selenium fallback to refresh cookies.
    Returns a DataFrame similar to your expected CSV format.
    """
    url = f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol}"
    user_agent = get_random_user_agent()
    headers = {
        "User-Agent": user_agent,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}",
        "Connection": "keep-alive",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "sec-ch-ua": "\" Not A;Brand\";v=\"99\", \"Chromium\";v=\"90\", \"Google Chrome\";v=\"90\"",
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin"
    }

    session = requests.Session()
    session.headers.update(headers)
    home_url = f"https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}"
    try:
        session.get(home_url, timeout=5)
    except Exception as e:
        print(f"Initial connection failed for {symbol}: {e}")
        return pd.DataFrame()
    
    try:
        response = session.get(url, timeout=10)
        if response.status_code != 200:
            print(f"Failed to fetch data for {symbol}, status code {response.status_code}")
            # If we get a 403 and Selenium is available, attempt fallback.
            if response.status_code == 403 and webdriver is not None:
                print("Attempting fallback using Selenium to get cookies...")
                selenium_cookies = get_cookies_via_selenium(symbol, user_agent)
                if selenium_cookies:
                    session.cookies.update(selenium_cookies)
                    response = session.get(url, timeout=10)
                    if response.status_code != 200:
                        print(f"Fallback failed for {symbol}, status code {response.status_code}")
                        return pd.DataFrame()
                else:
                    return pd.DataFrame()
            else:
                return pd.DataFrame()
        
        data = response.json()
        records = data.get("records", {}).get("data", [])
        rows = []
        for record in records:
            # Process Call (CE) options data if available
            if "CE" in record and record["CE"]:
                rows.append({
                    "INSTRUMENT": "OPTSTK",
                    "SYMBOL": symbol,
                    "OPTION_TYP": "CE",
                    "STRIKE_PR": record["CE"].get("strikePrice", None),
                    "EXPIRY_DT": record["CE"].get("expiryDate", None),
                    "CONTRACTS": record["CE"].get("openInterest", 0)
                })
            # Process Put (PE) options data if available
            if "PE" in record and record["PE"]:
                rows.append({
                    "INSTRUMENT": "OPTSTK",
                    "SYMBOL": symbol,
                    "OPTION_TYP": "PE",
                    "STRIKE_PR": record["PE"].get("strikePrice", None),
                    "EXPIRY_DT": record["PE"].get("expiryDate", None),
                    "CONTRACTS": record["PE"].get("openInterest", 0)
                })
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"Error parsing data for {symbol}: {e}")
        return pd.DataFrame()

def fetch_and_save_bhavcopy():
    """
    Iterates over the nifty50_symbols, fetches their option chain data with randomized delays,
    and saves the combined data to 'fo_bhav.csv'.
    """
    all_data = []
    for symbol in nifty50_symbols:
        print(f"Fetching option chain data for {symbol}...")
        df_symbol = fetch_option_chain(symbol)
        if not df_symbol.empty:
            all_data.append(df_symbol)
        # Random delay between 4 to 7 seconds between requests.
        time.sleep(random.uniform(4, 7))
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv("fo_bhav.csv", index=False)
        print("Option chain data saved to fo_bhav.csv")
    else:
        print("No data fetched.")

def update_pcr_database():
    """
    Reads the option chain data from 'fo_bhav.csv', processes PCR calculations for each symbol,
    and updates the Excel database.
    If no CSV file is available, exits gracefully.
    """
    if not os.path.exists("fo_bhav.csv"):
        fetch_and_save_bhavcopy()

    if not os.path.exists("fo_bhav.csv"):
        print("No fo_bhav.csv file was created; exiting update_pcr_database.")
        return

    df = pd.read_csv("fo_bhav.csv")
    # Retain only stock options data.
    df = df[df["INSTRUMENT"].isin(["OPTSTK"])]

    for symbol in nifty50_symbols:
        df_sym = df[df["SYMBOL"] == symbol]
        if df_sym.empty:
            continue

        pe_vol = df_sym[df_sym["OPTION_TYP"] == "PE"].groupby("STRIKE_PR")[["CONTRACTS"]].sum().rename(
            columns={"CONTRACTS": "PUT_CONTRACTS"}
        )
        ce_vol = df_sym[df_sym["OPTION_TYP"] == "CE"].groupby("STRIKE_PR")[["CONTRACTS"]].sum().rename(
            columns={"CONTRACTS": "CALL_CONTRACTS"}
        )
        df_pcr = pd.concat([pe_vol, ce_vol], axis=1).fillna(0)
        df_pcr["PCR_RATIO"] = df_pcr["PUT_CONTRACTS"] / df_pcr["CALL_CONTRACTS"].replace(0, np.nan)
        df_pcr.reset_index(inplace=True)
        df_pcr["DATE"] = today_date
        df_pcr["COMPANY"] = symbol
        df_pcr["EXPIRY_DATE"] = df_sym["EXPIRY_DT"].iloc[0]

        sheet_name = symbol
        if os.path.exists(DB_PATH):
            try:
                historical = pd.read_excel(DB_PATH, sheet_name=sheet_name)
            except Exception as e:
                historical = pd.DataFrame()
        else:
            historical = pd.DataFrame()

        historical = historical.sort_values("DATE")
        last_4 = historical.tail(4)
        combined = pd.concat([last_4, df_pcr], ignore_index=True)
        combined.sort_values("DATE", inplace=True)

        combined["PCR_5DAY_AVG"] = combined["PCR_RATIO"].rolling(window=5).mean()
        combined["PCR_deviation"] = combined["PCR_RATIO"] - combined["PCR_5DAY_AVG"]
        combined["PCR_ZSCORE"] = combined["PCR_deviation"] / combined["PCR_deviation"].rolling(window=5).std()
        combined["PCR_SPIKE_DIP_SIGNAL"] = combined["PCR_ZSCORE"].apply(
            lambda x: "SPIKE" if x > 2 else ("DIP" if x < -2 else "NORMAL")
        )
        combined["PCR_flag"] = combined["PCR_RATIO"].apply(
            lambda x: "HIGH_PCR" if x > 1.3 else ("LOW_PCR" if x < 0.7 else "NEUTRAL")
        )

        final = pd.concat([historical, df_pcr], ignore_index=True).drop_duplicates(subset=["DATE", "STRIKE_PR"])
        final = final.merge(
            combined[["STRIKE_PR", "DATE", "PCR_5DAY_AVG", "PCR_deviation", "PCR_ZSCORE", "PCR_SPIKE_DIP_SIGNAL", "PCR_flag"]],
            on=["STRIKE_PR", "DATE"],
            how="left"
        )

        with pd.ExcelWriter(DB_PATH, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            final.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"Updated PCR data for {symbol} on {today_date}")
        time.sleep(random.uniform(2, 4))

if __name__ == "__main__":
    update_pcr_database()



