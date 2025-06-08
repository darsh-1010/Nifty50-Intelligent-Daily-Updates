import os
import time
import random
import numpy as np
import pandas as pd
import requests
from datetime import datetime
from openpyxl import load_workbook
import logging

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
except ImportError:
    webdriver = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

today_date = datetime.today().strftime("%Y-%m-%d")
DB_PATH = 'Databases/Nifty_50_PCR_Hisotrical_Data.xlsx'
nifty50_symbols = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV",
    "BPCL", "BHARTIARTL", "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY", "EICHERMOT", "GRASIM", "HCLTECH",
    "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK", "INFY",
    "JSWSTEEL", "KOTAKBANK", "LT", "M&M", "MARUTI", "NTPC", "NESTLEIND", "ONGC", "POWERGRID", "RELIANCE", "SBILIFE",
    "SBIN", "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM", "TITAN", "UPL", "ULTRACEMCO", "WIPRO"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
]

def get_random_user_agent():
    return random.choice(USER_AGENTS)

def get_cookies_via_selenium(symbol, user_agent):
    home_url = f"https://www.nseindia.com/get-quotes/derivatives?symbol={symbol}"
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument(f"user-agent={user_agent}")
    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        logging.error(f"Selenium webdriver initialization failed for {symbol}: {e}")
        return {}
    try:
        driver.get(home_url)
        time.sleep(random.uniform(3, 6))
        cookies = driver.get_cookies()
    finally:
        driver.quit()
    cookie_dict = {}
    for cookie in cookies:
        cookie_dict[cookie["name"]] = cookie["value"]
    return cookie_dict

def fetch_option_chain(symbol):
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
        logging.error(f"Initial connection failed for {symbol}: {e}")
        return pd.DataFrame()
    try:
        response = session.get(url, timeout=10)
        if response.status_code != 200:
            logging.warning(f"Failed to fetch data for {symbol}, status code {response.status_code}")
            if response.status_code == 403 and webdriver is not None:
                logging.info("Attempting fallback using Selenium to get cookies...")
                selenium_cookies = get_cookies_via_selenium(symbol, user_agent)
                if selenium_cookies:
                    session.cookies.update(selenium_cookies)
                    response = session.get(url, timeout=10)
                    if response.status_code != 200:
                        logging.error(f"Fallback failed for {symbol}, status code {response.status_code}")
                        return pd.DataFrame()
                else:
                    return pd.DataFrame()
            else:
                return pd.DataFrame()
        data = response.json()
        records = data.get("records", {}).get("data", [])
        rows = []
        for record in records:
            if "CE" in record and record["CE"]:
                rows.append({
                    "INSTRUMENT": "OPTSTK",
                    "SYMBOL": symbol,
                    "OPTION_TYP": "CE",
                    "STRIKE_PR": record["CE"].get("strikePrice", None),
                    "EXPIRY_DT": record["CE"].get("expiryDate", None),
                    "CONTRACTS": record["CE"].get("openInterest", 0)
                })
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
        logging.error(f"Error parsing data for {symbol}: {e}")
        return pd.DataFrame()

def fetch_and_save_bhavcopy():
    all_data = []
    for symbol in nifty50_symbols:
        logging.info(f"Fetching option chain data for {symbol}...")
        df_symbol = fetch_option_chain(symbol)
        if not df_symbol.empty:
            all_data.append(df_symbol)
        time.sleep(random.uniform(4, 7))
    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv("fo_bhav.csv", index=False)
        logging.info("Option chain data saved to fo_bhav.csv")
    else:
        logging.warning("No data fetched.")

def update_pcr_database():
    if not os.path.exists("fo_bhav.csv"):
        fetch_and_save_bhavcopy()
    if not os.path.exists("fo_bhav.csv"):
        logging.error("No fo_bhav.csv file was created; exiting update_pcr_database.")
        return
    df = pd.read_csv("fo_bhav.csv")
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
        logging.info(f"Updated PCR data for {symbol} on {today_date}")
        time.sleep(random.uniform(2, 4))

if __name__ == "__main__":
    update_pcr_database()



