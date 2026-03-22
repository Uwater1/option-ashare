import pandas as pd
import numpy as np
from datetime import timedelta
import os
import matplotlib.pyplot as plt

# Configuration: Index data files from analyze_witching_days.py
FILES = {
    'SZ50 (SSE 50)': 'sz50_1d.csv', 
    'HS300 (CSI 300)': 'hs300_1d.csv',
    'ZZ500 (CSI 500)': 'zz500_1d.csv',
    'ZZ1000 (CSI 1000)': 'zz1000_1d.csv'
}

# Bucket sizes configuration
BUCKET_SIZES = {
    'SZ50 (SSE 50)': 50,
    'HS300 (CSI 300)': 50,
    'ZZ500 (CSI 500)': 100,
    'ZZ1000 (CSI 1000)': 100
}

START_DATE = '2010-04-16'

def load_data(file_path):
    """Loads and cleans data based on the format detected."""
    if not os.path.exists(file_path):
        return None
        
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
    return df

def calculate_30d_returns(df):
    """Calculates 30 calendar day forward returns."""
    df = df.set_index('Date')
    all_dates = df.index
    
    results = []
    for current_date in all_dates:
        target_date = current_date + timedelta(days=30)
        future_trading_days = all_dates[all_dates >= target_date]
        
        if not future_trading_days.empty:
            actual_future_date = future_trading_days[0]
            price_now = df.loc[current_date, 'Close']
            price_future = df.loc[actual_future_date, 'Close']
            
            results.append({
                'Date': current_date,
                'Price_Now': price_now,
                'Price_30d': price_future,
                'Point_Diff': price_future - price_now,
                'Return_Pct': (price_future - price_now) / price_now
            })
            
    return pd.DataFrame(results)

def get_distribution(df, bucket_size):
    """Buckets the point differences and calculates statistics."""
    df['Bucket'] = np.floor(df['Point_Diff'] / bucket_size).astype(int)
    df['Bucket_Range'] = df['Bucket'].apply(lambda x: f"[{x*bucket_size}, {(x+1)*bucket_size})")
    
    dist = df.groupby(['Bucket', 'Bucket_Range']).size().reset_index(name='Count')
    total = dist['Count'].sum()
    dist['Probability (%)'] = (dist['Count'] / total) * 100
    
    return dist.sort_values('Bucket')

def plot_distributions(all_returns, bucket_sizes):
    """Generates histogram plots for the return distributions."""
    num_indices = len(all_returns)
    if num_indices == 0: return
    
    fig, axes = plt.subplots(num_indices, 1, figsize=(12, 5 * num_indices))
    if num_indices == 1: axes = [axes]
    
    for i, (name, df) in enumerate(all_returns.items()):
        ax = axes[i]
        bs = bucket_sizes[name]
        
        # Determine range for plotting (central 95%)
        std = df['Point_Diff'].std()
        plot_range = (-4*std, 4*std)
        
        ax.hist(df['Point_Diff'], bins=np.arange(plot_range[0], plot_range[1], bs), 
                alpha=0.7, color='steelblue', edgecolor='black')
        
        ax.axvline(0, color='red', linestyle='--', alpha=0.5)
        ax.axvline(bs, color='green', linestyle=':', label=f'+{bs}pts')
        ax.axvline(-bs, color='orange', linestyle=':', label=f'-{bs}pts')
        
        ax.set_title(f"30-Day Forward Point Move Distribution: {name}")
        ax.set_xlabel("Point Difference")
        ax.set_ylabel("Frequency")
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig('alpha_30d_distribution.png')
    print("\nSaved distribution plots to alpha_30d_distribution.png")

def run_analysis():
    summary_stats = []
    all_returns_data = {}
    
    for name, path in FILES.items():
        print(f"\nProcessing {name}...")
        df_raw = load_data(path)
        if df_raw is None:
            print(f"Skipping {name}: file not found.")
            continue
            
        df_raw = df_raw[df_raw['Date'] >= pd.to_datetime(START_DATE)]
        if df_raw.empty:
            print(f"Skipping {name}: no data after {START_DATE}.")
            continue
            
        returns_df = calculate_30d_returns(df_raw)
        all_returns_data[name] = returns_df
        bucket_size = BUCKET_SIZES[name]
        dist = get_distribution(returns_df, bucket_size)
        
        print(f"30-Day Return Distribution (Bucket Size: {bucket_size} pts):")
        central = dist[(dist['Bucket'] >= -3) & (dist['Bucket'] <= 2)]
        print(central[['Bucket_Range', 'Count', 'Probability (%)']].to_string(index=False))
        
        csv_name = f"{name.split(' ')[0]}_30d_dist.csv"
        dist.to_csv(csv_name, index=False)
        
        summary_stats.append({
            'Index': name,
            '30d Avg Point Move': returns_df['Point_Diff'].mean(),
            '30d Std Dev Points': returns_df['Point_Diff'].std(),
            '30d Avg Return %': returns_df['Return_Pct'].mean() * 100,
            f'Prob Move < -{bucket_size}pts (%)': dist[dist['Bucket'] < 0]['Probability (%)'].sum(),
            f'Prob Move > +{bucket_size}pts (%)': dist[dist['Bucket'] >= 1]['Probability (%)'].sum()
        })

    summary_df = pd.DataFrame(summary_stats)
    print("\n=== Summary of 30-Day Moves ===")
    print(summary_df.round(2).to_string(index=False))
    
    if all_returns_data:
        plot_distributions(all_returns_data, BUCKET_SIZES)
    
if __name__ == "__main__":
    run_analysis()
