import akshare as ak
import time
import os
import yfinance as yf
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError

symbols = [
    "沪深300股指期权",
    "中证1000股指期权",
    "上证50股指期权"
]

# Mapping of option symbols to their underlying Yahoo Finance tickers
underlying_map = {
    "沪深300股指期权": "000300.SS",
    "中证1000股指期权": "000852.SS",
    "上证50股指期权": "000016.SS"
}

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

def get_spot_prices(underlying_map):
    """Fetch current spot prices for given underlying tickers."""
    spot_prices = {}
    for opt_sym, ticker in underlying_map.items():
        try:
            print(f"Fetching spot price for {opt_sym} ({ticker})...")
            df = yf.download(ticker, period="1d", progress=False)
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    close_price = df['Close', ticker].iloc[-1]
                else:
                    close_price = df['Close'].iloc[-1]
                spot_prices[opt_sym] = round(float(close_price), 3)
                print(f"Fetched spot price: {spot_prices[opt_sym]}")
            else:
                print(f"No price data found for {ticker}")
        except Exception as e:
            print(f"Error fetching spot price for {ticker}: {e}")
        time.sleep(1)
    return spot_prices

def fetch_and_save_daily_stats(date_str=None, exchanges=['sse', 'szse']):
    """Fetch and save daily trading statistics for SSE and SZSE. Returns list of failed exchanges."""
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")
    
    # Organize by date subdirectory
    output_dir = os.path.join("option_data", date_str)
    os.makedirs(output_dir, exist_ok=True)
    
    failed_exchanges = []

    # Shanghai Stock Exchange
    if 'sse' in exchanges:
        sse_filename = f"daily_stats_sse_{date_str}.csv"
        sse_filepath = os.path.join(output_dir, sse_filename)
        if os.path.exists(sse_filepath):
            print(f"File already exists, skipping SSE download: {sse_filepath}")
        else:
            try:
                print(f"Fetching Daily Stats for SSE: {date_str}")
                sse_df = ak.option_daily_stats_sse(date=date_str)
                if sse_df is not None and not sse_df.empty:
                    sse_df = sse_df.round(3)
                    sse_df.to_csv(sse_filepath, index=False, encoding="utf_8_sig")
                    print(f"Successfully saved to {sse_filepath}")
                else:
                    print(f"No SSE daily stats data returned for {date_str}")
                    failed_exchanges.append('sse')
            except Exception as e:
                print(f"Error fetching SSE daily stats: {e}")
                failed_exchanges.append('sse')

    # Shenzhen Stock Exchange
    if 'szse' in exchanges:
        szse_filename = f"daily_stats_szse_{date_str}.csv"
        szse_filepath = os.path.join(output_dir, szse_filename)
        if os.path.exists(szse_filepath):
            print(f"File already exists, skipping SZSE download: {szse_filepath}")
        else:
            try:
                print(f"Fetching Daily Stats for SZSE: {date_str}")
                szse_df = ak.option_daily_stats_szse(date=date_str)
                if szse_df is not None and not szse_df.empty:
                    szse_df = szse_df.round(3)
                    szse_df.to_csv(szse_filepath, index=False, encoding="utf_8_sig")
                    print(f"Successfully saved to {szse_filepath}")
                else:
                    print(f"No SZSE daily stats data returned for {date_str}")
                    failed_exchanges.append('szse')
            except Exception as e:
                print(f"Error fetching SZSE daily stats: {e}")
                failed_exchanges.append('szse')

    return failed_exchanges

def fetch_with_timeout(symbol, end_month):
    """Wrapper function to fetch option board for a single symbol and month."""
    return ak.option_finance_board(symbol=symbol, end_month=end_month)

def fetch_and_save_options(symbol_list, spot_prices, iterations=3):
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

                        trade_date = current_trade_date
                        
                        # Organize by date subdirectory
                        data_dir = os.path.join(output_base_dir, trade_date)
                        os.makedirs(data_dir, exist_ok=True)
                        
                        # Filename: {symbol}_{month}_{today}.csv
                        filename = f"{symbol}_{month}_{trade_date}.csv"
                        filepath = os.path.join(data_dir, filename)
                        
                        # Add spot price column
                        if symbol in spot_prices:
                            df['spot_price'] = spot_prices[symbol]
                        
                        # Round to 3 decimal places
                        df = df.round(3)
                        
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
    # Get today's date for folder and file organization
    today_str = datetime.now().strftime("%Y%m%d")
    output_dir = os.path.join("option_data_full", today_str)
    os.makedirs(output_dir, exist_ok=True)

    # 1. Fetch and save daily stats for today
    failed_daily_stats = fetch_and_save_daily_stats()
    
    # 2. Fetch current spot prices
    spot_prices = get_spot_prices(underlying_map)

    # 3. Proceed with periodic board data fetching (now with multiple expiration months)
    fetch_and_save_options(symbols, spot_prices, iterations=3)

    # 4. Final retry for daily stats if any failed initially
    if failed_daily_stats:
        print(f"Retrying failed daily stats for: {failed_daily_stats}")
        fetch_and_save_daily_stats(exchanges=failed_daily_stats)
