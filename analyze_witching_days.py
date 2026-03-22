import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Configuration: Index data files
FILES = {
    'SZ50 (SSE 50)': 'sz50_1d.csv', 
    'HS300 (CSI 300)': 'hs300_1d.csv',
    'ZZ500 (CSI 500)': 'zz500_1d.csv',
    'ZZ1000 (CSI 1000)': 'zz1000_1d.csv' # Did not trade until July 2022
}

START_DATE = '2010-04-16' #China introduced its first stock-index futures on April 16, 2010

def get_target_dates(start_year, end_year):
    """Calculates 4th Wednesdays (Options) and 3rd Fridays (Futures) using Pandas."""
    start = f"{start_year}-01-01"
    end = f"{end_year}-12-31"
    
    # WOM (Week of Month) frequencies: 4th Wed and 3rd Fri
    option_dates = pd.date_range(start, end, freq='WOM-4WED')
    future_dates = pd.date_range(start, end, freq='WOM-3FRI')
    
    # Construct results using pandas Timestamps for consistency
    dates = [('4th_Wed (Option)', d) for d in option_dates] + \
            [('3rd_Fri (Futures)', d) for d in future_dates]
            
    return sorted(dates, key=lambda x: x[1])


def load_data(file_path):
    with open(file_path, 'r') as f:
        first_line = f.readline()
    if first_line.startswith('Price,Close'):
        df = pd.read_csv(file_path, skiprows=3, names=['Date', 'Close', 'High', 'Low', 'Open', 'Volume'])
    elif first_line.startswith('time,open'):
        df = pd.read_csv(file_path)
        df = df.rename(columns={'time': 'Date', 'close': 'Close', 'open': 'Open', 'high': 'High', 'low': 'Low', 'Volume': 'Volume'})
    else:
        df = pd.read_csv(file_path)
        for col in df.columns:
            if 'date' in col.lower() or 'time' in col.lower(): df = df.rename(columns={col: 'Date'})
            if 'close' in col.lower(): df = df.rename(columns={col: 'Close'})
    
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date').reset_index(drop=True)
    df['Return'] = df['Close'].pct_change()
    
    # Filter after calculating returns
    start_ts = pd.to_datetime(START_DATE)
    df = df[df['Date'] >= start_ts].reset_index(drop=True)
    return df

def get_bucket_stats(rets):
    """Calculates custom probability buckets for returns."""
    if rets.empty:
        return {}
    return {
        'Count': int(len(rets)),
        'Mean (%)': rets.mean() * 100,
        'Std (%)': rets.std() * 100,
        'WinRate (%)': (rets > 0).mean() * 100,
        '< -2% (%)': (rets < -0.02).mean() * 100,
        '< -1% (%)': (rets < -0.01).mean() * 100,
        '> 1% (%)': (rets > 0.01).mean() * 100,
        '> 2% (%)': (rets > 0.02).mean() * 100,
        '|x| < 0.5% (%)': ((rets > -0.005) & (rets < 0.005)).mean() * 100
    }

def analyze():
    all_rows = []
    for name, path in FILES.items():
        if not os.path.exists(path): continue
        df = load_data(path)
        trading_dates = sorted(df['Date'].unique())
        trading_dates_set = set(trading_dates)
        
        target_dates = get_target_dates(df['Date'].dt.year.min(), df['Date'].dt.year.max())
        all_rets = df['Return'].dropna()
        
        # Baseline
        stats = get_bucket_stats(all_rets)
        stats['Index'] = name
        stats['Period'] = 'All Trading Days'
        all_rows.append(stats)
        
        # Target Days
        witching_data = []
        start_ts = pd.to_datetime(START_DATE)
        for type_name, dt in target_dates:
            # Skip target dates before our analysis starts
            if dt < start_ts:
                continue
                
            actual_dt = dt
            while actual_dt not in trading_dates_set and actual_dt <= trading_dates[-1]:
                actual_dt += timedelta(days=1)
            
            if actual_dt in trading_dates_set:
                ret = df[df['Date'] == actual_dt]['Return'].values[0]
                if not np.isnan(ret):
                    witching_data.append({'Type': type_name, 'Return': ret})
        
        w_df = pd.DataFrame(witching_data)
        for t in w_df['Type'].unique():
            t_rets = w_df[w_df['Type'] == t]['Return']
            t_stats = get_bucket_stats(t_rets)
            t_stats['Index'] = name
            t_stats['Period'] = t
            all_rows.append(t_stats)
            
    return pd.DataFrame(all_rows)

if __name__ == "__main__":
    results = analyze()
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print("\n=== Custom Bucket Distribution Analysis (Volatility Included) ===")
    cols = ['Index', 'Period', 'Count', 'Mean (%)', 'Std (%)', 'WinRate (%)', '< -2% (%)', '< -1% (%)', '> 1% (%)', '> 2% (%)', '|x| < 0.5% (%)']
    print(results[cols].round(2))
    
    results.to_csv('/home/hallo/Documents/display/witching_day_custom_buckets.csv', index=False)
