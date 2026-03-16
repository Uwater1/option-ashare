import os
import akshare as ak
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta

# Mapping of option symbols to their CFFEX futures prefixes
futures_prefix_map = {
    "沪深300股指期权": "IF",
    "中证1000股指期权": "IM",
    "上证50股指期权": "IH"
}

# Cache for historical futures data to avoid redundant API calls
futures_cache = {} # Key: contract_code, Value: {date_str: price}

def get_historical_future_price(symbol, month_str, trade_date_str):
    prefix = futures_prefix_map.get(symbol)
    if not prefix: return np.nan
    contract = f"{prefix}{month_str[2:]}".upper()
    if contract not in futures_cache:
        try:
            df = ak.futures_zh_daily_sina(symbol=contract)
            df['date'] = pd.to_datetime(df['date']).dt.strftime("%Y%m%d")
            futures_cache[contract] = dict(zip(df['date'], df['close']))
        except: futures_cache[contract] = {}
    return futures_cache[contract].get(trade_date_str, np.nan)

def get_expiry_date(month_str):
    """Calculate the expiry date (3rd Friday of the month)."""
    year = int(month_str[:4])
    month = int(month_str[4:])
    first_day = date(year, month, 1)
    days_to_fri = (4 - first_day.weekday() + 7) % 7
    first_friday = first_day + timedelta(days=days_to_fri)
    expiry_date = first_friday + timedelta(weeks=2)
    return expiry_date

def update_csv_files(target_dir):
    if not os.path.exists(target_dir):
        print(f"Directory {target_dir} not found.")
        return

    # Extract trade date from directory name
    trade_date_str = os.path.basename(target_dir)
    try:
        ref_date = datetime.strptime(trade_date_str, "%Y%m%d").date()
    except ValueError:
        print(f"Could not parse trade date from directory name: {trade_date_str}")
        return

    print(f"Updating files in {target_dir} for trade date {ref_date}...")

    for filename in os.listdir(target_dir):
        # We only update the option board files, not the stats files
        # Option files look like: {symbol}_{month}_{date}.csv
        if filename.endswith(".csv") and not (filename.startswith("stats_") or filename.startswith("daily_stats_") or filename.startswith("spot_prices_")):
            filepath = os.path.join(target_dir, filename)
            
            # Extract expiration month from filename (format: symbol_month_date.csv)
            parts = filename.replace(".csv", "").split("_")
            if len(parts) < 2:
                continue
                
            symbol = parts[0]
            month_str = parts[1] # e.g., 202603
            
            try:
                df = pd.read_csv(filepath)
                
                # Check if ticker column exists to confirm it's an option file we've processed
                if "ticker" not in df.columns:
                    print(f"Skipping {filename}: 'ticker' column not found.")
                    continue

                # Remove days_to_expire if it already exists to avoid duplicates
                if "days_to_expire" in df.columns:
                    df.drop(columns=["days_to_expire"], inplace=True)
                
                # Add or update future_price
                f_price = get_historical_future_price(symbol, month_str, trade_date_str)
                df['future_price'] = f_price

                # Calculate days_to_expire
                expiry_date = get_expiry_date(month_str)
                days_to_expire = int((expiry_date - ref_date).days)
                
                # Insert at position 3 (after ticker, type, strike)
                df.insert(3, "days_to_expire", days_to_expire)
                
                # Save back
                df.to_csv(filepath, index=False, encoding="utf_8_sig")
                print(f"Updated {filename}: days_to_expire = {days_to_expire}")
                
            except Exception as e:
                print(f"Error processing {filename}: {e}")

if __name__ == "__main__":
    base_data_dir = "/home/hallo/Documents/option-ashare/option_data"
    if os.path.exists(base_data_dir):
        for entry in os.listdir(base_data_dir):
            target_path = os.path.join(base_data_dir, entry)
            if os.path.isdir(target_path) and entry.isdigit(): # Only process yyyymmdd dirs
                update_csv_files(target_path)
    else:
        print(f"Base data directory {base_data_dir} not found.")
