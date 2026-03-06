# 📉 Option A-Share Downloader

Automated tool to fetch and archive Chinese A-share index option data.

## 🚀 Overview

This project provides a robust solution for downloading daily trading statistics and option board data for major Chinese stock index options. It uses `akshare` for data retrieval and `yfinance` for underlying spot prices.

## 🛠️ Key Components

### 1. `option.py` (Main Script)
The core engine of the downloader. It performs the following tasks:
- **Daily Stats**: Fetches daily trading statistics for Shanghai (SSE) and Shenzhen (SZSE) exchanges.
- **Spot Prices**: Retrieves real-time spot prices for underlying indices (滬深300, 中證1000, 上證50) via Yahoo Finance.
- **Option Boards**: Downloads the option chain data for the 4 standard expiration months (current, next, and next two quarterly months).
- **Data Integrity**: Includes retry logic, timeout handling, and checks to avoid redundant downloads.
- **Organization**: Automatically saves data into `option_data/YYYYMMDD/` subdirectories with CSV files encoded in `utf-8-sig` for Excel compatibility.

### 2. GitHub Action (`daily_fetch.yml`)
Located in `.github/workflows/`, this workflow automates the entire process:
Runs daily at **15:30 Beijing Time** (07:30 UTC), shortly after the market close.

## 📂 Data Structure
```text
option_data/
└── YYYYMMDD/
    ├── daily_stats_sse_YYYYMMDD.csv
    ├── daily_stats_szse_YYYYMMDD.csv
    └── [Symbol]_[Month]_YYYYMMDD.csv
```

## 📜 Usage
```bash
python option.py # download all option data from A stock for today, may encounter rate limit

python index.py # download all index data from A stock for today
```
---
> [!NOTE]  
> This project is for educational and research purposes only. Always verify financial data before making investment decisions.
