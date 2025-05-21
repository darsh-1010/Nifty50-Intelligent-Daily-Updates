# 📊 Nifty50 Intelligent Daily Updates (Automated with AI)

This repository provides a fully automated pipeline that updates and maintains key datasets related to the **Nifty 50 index**, using Python scripts, AI-based analysis, and GitHub Actions for daily automation.

## 🔍 Overview

- 🧠 **AI-driven sentiment analysis** from live news
- 🧮 **Options data processing** with Put-Call Ratio (PCR)
- 📈 **OHLCV & technical indicator generation** for all Nifty 50 stocks
- 🔁 **Daily auto-updates via GitHub Actions**
- 📁 Results saved in version-controlled Excel files in the `databases/` folder

> This project is ideal for analysts, quants, and AI/ML engineers working in financial modeling, algo trading, or data science.

---

## 🧠 Features

### 1. 📈 OHLCV Data + Technical Indicators
- Fetches EOD (end-of-day) data from Yahoo Finance
- Computes advanced technical indicators using `TA-Lib` and `pandas-ta`
- Applies custom feature engineering to enhance modeling

### 2. 📰 News Sentiment Analysis
- Uses `transformers` and `torch` to analyze financial news
- Performs entity-level tagging and sentiment classification
- Aggregates daily sentiment into a score for each stock

### 3. 🔢 PCR (Put-Call Ratio) Extraction
- Collects and computes historical and current PCR values
- Uses live data scraping (ethically and responsibly)
- Helps gauge market sentiment from derivatives data

### 4. 🤖 Daily Auto Updates (CI/CD)
- GitHub Actions triggers every day at 8:00 AM IST
- Runs update scripts automatically
- Pushes new Excel files to the `databases/` folder

---

## 🛠️ Tech Stack

| Layer        | Tools & Libraries                         |
|--------------|--------------------------------------------|
| Language     | Python 3.10                                |
| Data Fetch   | `yfinance`, `requests`, custom scraping    |
| AI/ML        | `transformers`, `torch`                    |
| Analysis     | `pandas`, `numpy`, `ta`, `openpyxl`        |
| Automation   | GitHub Actions (CI/CD pipeline)            |
| Storage      | Excel files (`.xlsx`) stored in GitHub     |

---

## 🗃️ File Structure
.
├── databases/ # Auto-updated Excel files
│ ├── nifty50_processed_features_updated_with_actions.xlsx
│ ├── final_combined_sentiment.xlsx
│ └── Nifty_50_PCR_Hisotrical_Data.xlsx
├── updateohlcv.py # Main script for OHLCV + indicators
├── UPDATE_NEWS.py # AI-based sentiment analysis
├── update_pcr.py # PCR data fetcher
├── requirements.txt # Python dependencies
└── .github/workflows/daily_update.yml # GitHub Actions workflow


---

## 🏁 How It Works

1. GitHub Actions triggers every morning (8 AM IST).
2. Python scripts fetch new data and perform transformations.
3. Excel files are updated and committed back to GitHub.
4. You get fresh, AI-enhanced datasets — every single day.

---

## 🚀 Getting Started (Local Use)

To run it locally:

bash
git clone https://github.com/your-username/Nifty50-Intelligent-Daily-Updates.git
cd Nifty50-Intelligent-Daily-Updates 

# Create a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

pip install -r requirements.txt'''

---------------------

# Run scripts manually
python updateohlcv.py

python UPDATE_NEWS.py

python update_pcr.py

-------------

## 📬 Contributing
Pull requests are welcome. For major changes, open an issue first to discuss what you would like to change.

## 📢 Keywords for Discoverability
nginx
Copy
Edit
Nifty 50, Stock Market, Technical Analysis, News Sentiment, PCR, Derivatives, Algo Trading, GitHub Actions, Python, Finance, AI, Quant, Machine Learning, Equity Data, Financial Engineering, Automated Pipelines, Stock Forecasting

-----------------------------------------

## 📄 License
This project is for educational and research purposes. Always ensure compliance with financial data usage terms and data provider policies.

