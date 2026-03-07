import os
import pandas as pd
from datetime import datetime, date, timedelta

def get_expiry_date(month_str):
    """Calculate the expiry date (4th Wednesday of the month)."""
    year = int(month_str[:4])
    month = int(month_str[4:])
    first_day = date(year, month, 1)
    days_to_wed = (2 - first_day.weekday() + 7) % 7
    first_wednesday = first_day + timedelta(days=days_to_wed)
    expiry_date = first_wednesday + timedelta(weeks=3)
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
        if filename.endswith(".csv") and not filename.startswith("stats_"):
            filepath = os.path.join(target_dir, filename)
            
            # Extract expiration month from filename (format: symbol_month_date.csv)
            parts = filename.replace(".csv", "").split("_")
            if len(parts) < 2:
                continue
                
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
    target_path = "/home/hallo/Documents/option-ashare/option_data/20260306"
    update_csv_files(target_path)
