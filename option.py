import akshare as ak
import time
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError

symbols = [
    "华夏上证50ETF期权",
    "华泰柏瑞沪深300ETF期权",
    "南方中证500ETF期权",
    "华夏科创50ETF期权",
    "易方达科创50ETF期权",
    "沪深300股指期权",
    "中证1000股指期权",
    "上证50股指期权"
]

def get_target_months():
    """Determine the 4 standard expiration months: current, next, and next 2 quarterly months."""
    now = datetime.now()
    year = now.year
    month = now.month
    
    target_months = []
    
    # 1. Current month
    target_months.append(f"{year}{month:02d}")
    
    # 2. Next month
    nm = month + 1
    ny = year
    if nm > 12:
        nm = 1
        ny += 1
    target_months.append(f"{ny}{nm:02d}")
    
    # 3 & 4. Subsequent two quarterly months (3, 6, 9, 12)
    quarters = [3, 6, 9, 12]
    found = 0
    curr_m, curr_y = nm, ny
    while found < 2:
        curr_m += 1
        if curr_m > 12:
            curr_m = 1
            curr_y += 1
        if curr_m in quarters:
            target_months.append(f"{curr_y}{curr_m:02d}")
            found += 1
    return target_months

def fetch_and_save_daily_stats(date_str=None):
    """Fetch and save daily trading statistics for SSE and SZSE."""
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")
    
    # Organize by date subdirectory
    output_dir = os.path.join("option_data", date_str)
    os.makedirs(output_dir, exist_ok=True)
    
    # Shanghai Stock Exchange
    sse_filename = f"daily_stats_sse_{date_str}.csv"
    sse_filepath = os.path.join(output_dir, sse_filename)
    if os.path.exists(sse_filepath):
        print(f"File already exists, skipping SSE download: {sse_filepath}")
    else:
        try:
            print(f"Fetching Daily Stats for SSE: {date_str}")
            sse_df = ak.option_daily_stats_sse(date=date_str)
            if sse_df is not None and not sse_df.empty:
                sse_df.to_csv(sse_filepath, index=False, encoding="utf_8_sig")
                print(f"Successfully saved to {sse_filepath}")
            else:
                print(f"No SSE daily stats data returned for {date_str}")
        except Exception as e:
            print(f"Error fetching SSE daily stats: {e}")

    # Shenzhen Stock Exchange
    szse_filename = f"daily_stats_szse_{date_str}.csv"
    szse_filepath = os.path.join(output_dir, szse_filename)
    if os.path.exists(szse_filepath):
        print(f"File already exists, skipping SZSE download: {szse_filepath}")
    else:
        try:
            print(f"Fetching Daily Stats for SZSE: {date_str}")
            szse_df = ak.option_daily_stats_szse(date=date_str)
            if szse_df is not None and not szse_df.empty:
                szse_df.to_csv(szse_filepath, index=False, encoding="utf_8_sig")
                print(f"Successfully saved to {szse_filepath}")
            else:
                print(f"No SZSE daily stats data returned for {date_str}")
        except Exception as e:
            print(f"Error fetching SZSE daily stats: {e}")

def fetch_with_timeout(symbol, end_month):
    """Wrapper function to fetch option board for a single symbol and month."""
    return ak.option_finance_board(symbol=symbol, end_month=end_month)

def fetch_and_save_options(symbol_list, iterations=3):
    # Determine target months for all symbols
    target_months = get_target_months()
    print(f"Target expiration months: {target_months}")
    
    today_str = datetime.now().strftime("%Y%m%d")
    
    # Initialize task queue as (symbol, month) pairs
    remaining_tasks = []
    for symbol in symbol_list:
        for month in target_months:
            # Check if file already exists locally
            filename = f"{symbol}_{month}_{today_str}.csv"
            filepath = os.path.join("option_data", today_str, filename)
            if os.path.exists(filepath):
                print(f"File already exists, skipping initial task: {filepath}")
                continue
            remaining_tasks.append((symbol, month))
            
    output_base_dir = "option_data"
    os.makedirs(output_base_dir, exist_ok=True)
    
    # Use ThreadPoolExecutor to handle timeouts
    with ThreadPoolExecutor(max_workers=1) as executor:
        for i in range(iterations):
            if not remaining_tasks:
                print("All tasks successfully completed. Exiting.")
                break
                
            print(f"--- Iteration {i+1} ---")
            print(f"Remaining tasks: {len(remaining_tasks)}")
            
            successful_in_this_round = []
            for symbol, month in remaining_tasks:
                # Re-verify existence inside the loop (in case it was just saved)
                current_trade_date = datetime.now().strftime("%Y%m%d")
                filename = f"{symbol}_{month}_{current_trade_date}.csv"
                filepath = os.path.join(output_base_dir, current_trade_date, filename)
                if os.path.exists(filepath):
                    print(f"File exists, skipping: {filepath}")
                    successful_in_this_round.append((symbol, month))
                    continue

                print(f"Fetching: {symbol} for month {month}")
                try:
                    # Submit the fetch task with a 10s timeout
                    future = executor.submit(fetch_with_timeout, symbol, month)
                    df = future.result(timeout=10)
                    
                    if df is not None and not df.empty:
                        # Extract trade date for folder structure
                        if "日期" in df.columns:
                            timestamp = str(df["日期"].iloc[0])
                            trade_date = timestamp[:8]
                        else:
                            trade_date = current_trade_date
                        
                        # Organize by date subdirectory
                        data_dir = os.path.join(output_base_dir, trade_date)
                        os.makedirs(data_dir, exist_ok=True)
                        
                        # Filename: {symbol}_{month}_{today}.csv
                        filename = f"{symbol}_{month}_{trade_date}.csv"
                        filepath = os.path.join(data_dir, filename)
                        
                        # Save to CSV using utf_8_sig
                        df.to_csv(filepath, index=False, encoding="utf_8_sig")
                        print(f"Successfully saved to {filepath}")
                        
                        # Mark task as successful
                        successful_in_this_round.append((symbol, month))
                    else:
                        print(f"No data returned for {symbol} - {month}")
                        
                except TimeoutError:
                    print(f"Timeout: Fetching {symbol} {month} took too long (max 10s)")
                except Exception as e:
                    print(f"Error processing {symbol} {month}: {e}")
                
                # Sleep between requests to avoid rate limiting
                time.sleep(3)
            
            # Remove successful tasks from the queue
            for task in successful_in_this_round:
                remaining_tasks.remove(task)

if __name__ == "__main__":
    # First, fetch and save daily stats for today
    fetch_and_save_daily_stats()
    
    # Then proceed with periodic board data fetching (now with multiple expiration months)
    fetch_and_save_options(symbols, iterations=3)