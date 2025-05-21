import requests
import pandas as pd
import hashlib
from datetime import datetime, timedelta
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import os
 
# ---------------------- API KEYS ----------------------
NEWSDATA_API_KEY = "pub_79508cd750e96ccb61c080a4e004aca7439c3"
FINNHUB_API_KEY = "cvrokohr01qnpem8p570cvrokohr01qnpem8p57g"
GNEWS_API_KEY = "434440771cb4bd849b92821e037741ba"
 
# ---------------------- FINBERT SETUP ----------------------
tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
labels = ["Negative", "Neutral", "Positive"]
 
def classify_sentiment_finbert(text):
    inputs = tokenizer(text, return_tensors="pt", truncation=True)
    with torch.no_grad():
        outputs = model(**inputs)
    scores = torch.nn.functional.softmax(outputs.logits, dim=1).squeeze()
    sentiment_score = scores.tolist()
    sentiment = labels[sentiment_score.index(max(sentiment_score))]
    score = round(max(sentiment_score), 3)
    return sentiment, score if sentiment == "Positive" else (-score if sentiment == "Negative" else 0)
 
# ---------------------- NEWS FETCHERS ----------------------
def fetch_newsdata():
    url = "https://newsdata.io/api/1/latest"
    query = (
        "India stock market OR Indian economy OR SEBI OR NSE OR BSE OR Nifty OR Sensex OR RBI OR War OR "
        "India Pakistan conflict OR war OR China Taiwan OR oil prices OR inflation OR US Fed OR Trump tariffs OR "
        "global economy OR Middle East OR crash OR Infosys OR TCS OR Reliance OR HDFC OR ICICI OR SBI OR Adani OR Tata"
    )
    params = {
        'apikey': NEWSDATA_API_KEY,
        'q': query,
        'country': 'in',
        'language': 'en',
        'category': 'business'
    }
    res = requests.get(url, params=params)
    news = []
    if res.status_code == 200:
        data = res.json()
        for item in data.get("results", []):
            title = item.get("title", "")
            sentiment, score = classify_sentiment_finbert(title)
            news.append({
                "title": title,
                "published": item.get("pubDate"),
                "sentiment_score": score
            })
    return news
 
def fetch_finnhub(symbols=None):
    if symbols is None:
        symbols = ['RELIANCE.NS', 'NSEI', 'TCS.NS', 'INFY.NS', 'SBIN.NS', "HDFCBANK.NS"]
    all_news = []
    today = pd.Timestamp.now().strftime('%Y-%m-%d')
    for symbol in symbols:
        url = "https://finnhub.io/api/v1/company-news"
        params = {'symbol': symbol, 'from': today, 'to': today, 'token': FINNHUB_API_KEY}
        res = requests.get(url, params=params)
        if res.status_code == 200:
            data = res.json()
            for item in data:
                title = item.get("headline", "")
                sentiment, score = classify_sentiment_finbert(title)
                all_news.append({
                    "title": title,
                    "published": pd.to_datetime(item.get("datetime"), unit='s'),
                    "sentiment_score": score
                })
    return all_news
 
def fetch_gnews():
    query = (
        "India stock OR Indian finance OR SEBI OR IPO OR RBI OR Indian shares OR Nifty OR Sensex OR "
        "India Pakistan war OR border conflict OR Trump tariffs OR US Fed OR oil prices OR inflation OR "
        "Middle East tensions OR China Taiwan OR global economy"
    )
    url = "https://gnews.io/api/v4/search"
    params = {
        'q': query,
        'country': 'in',
        'lang': 'en',
        'token': GNEWS_API_KEY,
        'max': 50
    }
    res = requests.get(url, params=params)
    news = []
    if res.status_code == 200:
        data = res.json()
        for article in data.get("articles", []):
            title = article.get("title", "")
            sentiment, score = classify_sentiment_finbert(title)
            news.append({
                "title": title,
                "published": article.get("publishedAt"),
                "sentiment_score": score
            })
    return news
 
# ---------------------- UNIFIED FETCH ----------------------
def get_news():
    combined_news = fetch_newsdata() + fetch_finnhub() + fetch_gnews()
 
    # Deduplicate by title hash
    seen = set()
    unique_news = []
    for article in combined_news:
        h = hashlib.sha256(article['title'].encode()).hexdigest()
        if h not in seen:
            seen.add(h)
            unique_news.append(article)
 
    df = pd.DataFrame(unique_news)
    df['published'] = pd.to_datetime(df['published'], errors='coerce')
    df = df[df['published'].notna()]
   
    # Keep only today + yesterday
    now = pd.Timestamp.now()
    yesterday = now - timedelta(days=1)
    df = df[df['published'].between(yesterday.normalize(), now)]
 
    # Rename and reorder columns
    df['Date'] = df['published'].dt.strftime('%d-%m-%Y') + ' 00:00:00 '
    df['FinBERT_Sentiment'] = df['sentiment_score']
    df['Combined_Titles'] = df['title']
 
    df = df[['Date', 'FinBERT_Sentiment', 'Combined_Titles']].drop_duplicates()
    return df
 
# ---------------------- APPEND TO EXCEL WITHOUT OVERWRITE ----------------------
def append_new_rows_to_excel(new_data: pd.DataFrame, excel_path: str):
    if os.path.exists(excel_path):
        existing_df = pd.read_excel(excel_path)
        # Ensure column names match
        existing_df = existing_df[['Date', 'FinBERT_Sentiment', 'Combined_Titles']]
        # Remove duplicates based on title
        combined_df = pd.concat([existing_df, new_data], ignore_index=True)
        combined_df.drop_duplicates(subset='Combined_Titles', inplace=True)
        combined_df.to_excel(excel_path, index=False)
        print(f"✅ Appended {len(combined_df) - len(existing_df)} new rows to Excel.")
    else:
        new_data.to_excel(excel_path, index=False)
        print("✅ Excel file created with new data.")
 
# ---------------------- MAIN ----------------------
if __name__ == "__main__":
    df = get_news()
    excel_path = r"databases/final_combined_sentiment.xlsx"
    append_new_rows_to_excel(df, excel_path)
