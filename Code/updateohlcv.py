

# import yfinance as yf
# import pandas as pd
# import numpy as np
# from datetime import datetime, timedelta
# import os
# from ta.trend import MACD, EMAIndicator, ADXIndicator, PSARIndicator
# from ta.momentum import RSIIndicator
# from ta.volatility import BollingerBands, AverageTrueRange

# # Constants
# excel_path = r"Databases/nifty50_processed_features_updated_with_actions.xlsx"
# nifty50_tickers = [
#     "RELIANCE", "TCS", "HDFC", "INFY", "HDFCBANK", "ICICIBANK",
#     "KOTAKBANK", "BHARTIARTL", "ITC", "LT", "ASIANPAINT", "HINDUNILVR",
#     "MARUTI", "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "SBIN", "NTPC",
#     "POWERGRID", "ULTRACEMCO", "NESTLEIND", "BRITANNIA", "MandM",
#     "SUNPHARMA", "DIVISLAB", "INDUSINDBK", "TATAMOTORS", "TITAN",
#     "DRREDDY", "GRASIM", "ADANIPORTS", "ADANIENT", "ADANIGREEN", "ADANITRANS",
#     "VEDL", "SHREECEM", "BAJAJ-AUTO", "HEROMOTOCO", "WIPRO", "TECHM",
#     "COALINDIA", "BPCL", "GAIL", "IOC", "UPL", "EICHERMOT"
# ]
# tickers_yf = [ticker + ".NS" for ticker in nifty50_tickers]
# ticker_map = dict(zip(nifty50_tickers, tickers_yf))

# def calculate_features(df):
#     df = df.copy()
#     df['DATE'] = pd.to_datetime(df['DATE'])
#     df.sort_values('DATE', inplace=True)
#     df.set_index('DATE', inplace=True)

#     df['EMA20'] = EMAIndicator(df['CLOSE'], window=20).ema_indicator()
#     macd = MACD(df['CLOSE'])
#     df['MACD'] = macd.macd()
#     df['MACD_SIGNAL'] = macd.macd_signal()
#     df['MACD_HIST'] = macd.macd_diff()
#     df['RSI'] = RSIIndicator(df['CLOSE'], window=14).rsi()
#     bb = BollingerBands(df['CLOSE'])
#     df['BOLL_UPPER'] = bb.bollinger_hband()
#     df['BOLL_LOWER'] = bb.bollinger_lband()
#     df['ATR'] = AverageTrueRange(df['HIGH'], df['LOW'], df['CLOSE']).average_true_range()
#     df['PIVOT'] = (df['HIGH'] + df['LOW'] + df['CLOSE']) / 3
#     df['R1'] = 2 * df['PIVOT'] - df['LOW']
#     df['S1'] = 2 * df['PIVOT'] - df['HIGH']
#     df['R2'] = df['PIVOT'] + (df['HIGH'] - df['LOW'])
#     df['S2'] = df['PIVOT'] - (df['HIGH'] - df['LOW'])
#     df['R3'] = df['HIGH'] + 2 * (df['PIVOT'] - df['LOW'])
#     df['S3'] = df['LOW'] - 2 * (df['HIGH'] - df['PIVOT'])
#     diff = df['HIGH'] - df['LOW']
#     df['FIB_R1'] = df['CLOSE'] + 0.236 * diff
#     df['FIB_R2'] = df['CLOSE'] + 0.382 * diff
#     df['FIB_R3'] = df['CLOSE'] + 0.618 * diff
#     df['FIB_S1'] = df['CLOSE'] - 0.236 * diff
#     df['FIB_S2'] = df['CLOSE'] - 0.382 * diff
#     df['FIB_S3'] = df['CLOSE'] - 0.618 * diff
#     h_l = df['HIGH'] - df['LOW']
#     df['CAM_R1'] = df['CLOSE'] + h_l * 1.1 / 12
#     df['CAM_R2'] = df['CLOSE'] + h_l * 1.1 / 6
#     df['CAM_R3'] = df['CLOSE'] + h_l * 1.1 / 4
#     df['CAM_R4'] = df['CLOSE'] + h_l * 1.1 / 2
#     df['CAM_S1'] = df['CLOSE'] - h_l * 1.1 / 12
#     df['CAM_S2'] = df['CLOSE'] - h_l * 1.1 / 6
#     df['CAM_S3'] = df['CLOSE'] - h_l * 1.1 / 4
#     df['CAM_S4'] = df['CLOSE'] - h_l * 1.1 / 2
#     adx = ADXIndicator(df['HIGH'], df['LOW'], df['CLOSE'])
#     df['ADX'] = adx.adx()
#     df['ADX_Change'] = df['ADX'].diff()
#     sar = PSARIndicator(df['HIGH'], df['LOW'], df['CLOSE'])
#     df['SAR'] = sar.psar()
#     df['SAR_Diff'] = df['CLOSE'] - df['SAR']
#     df['Rolling_ADX_5'] = df['ADX'].rolling(window=5).mean()
#     df['Rolling_SAR_5'] = df['SAR_Diff'].rolling(window=5).mean()
#     df['Daily_Return'] = df['CLOSE'].pct_change(fill_method=None)
#     df['Log_Return'] = np.log(df['CLOSE'] / df['CLOSE'].shift(1))
#     df['Rolling_Mean_5'] = df['CLOSE'].rolling(window=5).mean()
#     df['Rolling_Std_5'] = df['CLOSE'].rolling(window=5).std()
#     df['Momentum_5'] = df['CLOSE'] - df['CLOSE'].shift(5)
#     df['Volatility_5'] = df['Daily_Return'].rolling(window=5).std()
#     df['Lag_1D'] = df['CLOSE'].shift(1)
#     df['Lag_3D'] = df['CLOSE'].shift(3)
#     df['Lag_5D'] = df['CLOSE'].shift(5)
#     df['Lag_10D'] = df['CLOSE'].shift(10)
#     df['RSI.1'] = df['RSI']
#     df['RSI_flag'] = np.select(
#         [df['RSI'] > 70, df['RSI'] < 30],
#         [1, -1],
#         default=0
#     )
#     mapping = {
#         0: ("Neutral", "Balanced", "No Signal"),
#         1: ("Overbought", "Too Many", "Sell"),
#         -1: ("Oversold", "Too Many", "Buy")
#     }
#     df[['Meaning', 'Market Mood', 'Possible Action']] = df['RSI_flag'].map(mapping).apply(pd.Series)
#     df['LABEL'] = df['RSI_flag'].map({-1: "BUY", 0: "HOLD", 1: "SELL"})
#     df.reset_index(inplace=True)
#     return df

# # Main logic
# if not os.path.exists(excel_path):
#     raise FileNotFoundError(f"{excel_path} does not exist!")

# xls = pd.ExcelFile(excel_path)
# sheet_names = xls.sheet_names
# updated_data = {}
# today = datetime.today().date()

# for sheet in sheet_names:
#     print(f"\n📈 Updating: {sheet}")
#     try:
#         existing_df = xls.parse(sheet)
#         existing_df.columns = [col.upper() if isinstance(col, str) else col[0].upper() for col in existing_df.columns]

#         if existing_df.empty or 'DATE' not in existing_df.columns:
#             print(f"⚠️ No data or 'DATE' column missing in {sheet}. Skipping.")
#             continue

#         existing_df['DATE'] = pd.to_datetime(existing_df['DATE'])
#         last_date = existing_df['DATE'].max().date()
#         fetch_from = last_date + timedelta(days=1)

#         if fetch_from > today:
#             print("✅ Already up to date.")
#             updated_data[sheet] = existing_df
#             continue

#         print(f"⏳ Fetching new data from {fetch_from} to {today}")
#         yf_data = yf.download(ticker_map[sheet], start=fetch_from, end=today + timedelta(days=1), interval='1d')

#         if yf_data.empty:
#             print("⚠️ No new data available.")
#             updated_data[sheet] = existing_df
#             continue

#         yf_data = yf_data.reset_index()[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
#         yf_data.columns = ['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
#         yf_data['SYMBOL'] = sheet
#         yf_data = yf_data[['DATE', 'SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']]

#         # Ensure same column names for merging
#         base_columns = ['DATE', 'SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
#         base_existing_df = existing_df[base_columns]

#         combined_df = pd.concat([base_existing_df, yf_data], ignore_index=True)
#         combined_df.drop_duplicates(subset='DATE', keep='last', inplace=True)

#         final_df = calculate_features(combined_df)
#         updated_data[sheet] = final_df

#         print(f"✅ Updated rows: {len(yf_data)}")
#     except Exception as e:
#         print(f"❌ Error updating {sheet}: {e}")

# # Save updated data to Excel
# with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
#     for sheet, df in updated_data.items():
#         df.to_excel(writer, sheet_name=sheet, index=False)

# print(f"\n✅ All sheets updated in: {excel_path}")
