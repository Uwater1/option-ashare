# 📉 Option A-Share Downloader

Automated tool to fetch and archive Chinese A-share index option data.

## 🚀 Overview

This project provides a robust solution for downloading daily trading statistics and option board data for major Chinese stock index options. It uses `akshare` for data retrieval and `yfinance` for underlying spot prices.

## 🛠️ Key Components

### 1. `index.py` (Main Script)
The core engine of the downloader. It performs the following tasks:
- **Daily Stats**: Fetches daily trading statistics for Shanghai (SSE) and Shenzhen (SZSE) exchanges.
- **Spot Prices**: Retrieves real-time spot prices for underlying indices (滬深300, 中證1000, 上證50) via Yahoo Finance.
- **Option Boards**: Downloads the option chain data for the 4 standard expiration months (current, next, and next two quarterly months).
- **Data Integrity**: Includes retry logic, timeout handling, and checks to avoid redundant downloads.
- **Organization**: Automatically saves data into `option_data/YYYYMMDD/` subdirectories with CSV files encoded in `utf-8-sig` for Excel compatibility.

### 2. `test.py` (Verification)
A minimal script used to verify that the `akshare` environment and basic API calls are working correctly.

### 3. GitHub Action (`daily_fetch.yml`)
Located in `.github/workflows/`, this workflow automates the entire process:
- **Schedule**: Runs daily at **15:30 Beijing Time** (07:30 UTC), shortly after the market close.
- **Automation**: Installs dependencies, runs `index.py`, captures execution logs, and automatically commits new data back to the repository.

## 📂 Data Structure
```text
option_data/
└── YYYYMMDD/
    ├── daily_stats_sse_YYYYMMDD.csv
    ├── daily_stats_szse_YYYYMMDD.csv
    └── [Symbol]_[Month]_YYYYMMDD.csv
```

## 📜 License & Usage

© 2026. All Rights Reserved.

- **Scripts**: The Python code (`index.py`, `test.py`) is licensed under **AGPL-3.0**.
- **Data**: Data found in `option_data/` is sourced from public financial providers and is **not** covered by the AGPL license. Users must comply with the terms of the original data sources.

---
> [!NOTE]  
> This project is for educational and research purposes only. Always verify financial data before making investment decisions.
