import pandas as pd
import matplotlib.pyplot as plt
import os
import glob
import sys
import numpy as np
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq

def black_scholes_price(S, K, T, r, sigma, option_type='C'):
    if T <= 0:
        return max(0, S - K) if option_type == 'C' else max(0, K - S)
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    if option_type == 'C':
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    else:
        return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def find_iv(market_price, S, K, T, r, option_type='C'):
    # Basic arbitrage boundaries check (relaxed slightly to allow for small market noise)
    intrinsic = max(0, S - K) if option_type == 'C' else max(0, K - S)
    if market_price <= intrinsic * 0.99: # Allow for tiny discount due to noise
        return np.nan
        
    try:
        def objective(sigma):
            return black_scholes_price(S, K, T, r, sigma, option_type) - market_price
        
        # Check if 1% or 500% vol brackets the solution
        if objective(1e-4) * objective(10.0) > 0:
            return np.nan
            
        return brentq(objective, 1e-4, 10.0)
    except:
        return np.nan

def plot_liquidity(csv_path):
    print(f"Processing {csv_path}...")
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading {csv_path}: {e}")
        return
        
    if 'type' not in df.columns or 'strike' not in df.columns or 'volume' not in df.columns or 'position' not in df.columns:
        print(f"Skipping {csv_path}: Missing required columns.")
        return

    # Separate into Call and Put
    calls = df[df['type'] == 'C'].sort_values('strike')
    puts = df[df['type'] == 'P'].sort_values('strike')
    
    if calls.empty and puts.empty:
        print(f"No option data found in {csv_path}.")
        return

    plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    fig, axes = plt.subplots(4, 1, figsize=(12, 14), sharex=True)
    
    # Determine bar width based on strike interval
    strikes = np.sort(df['strike'].unique())
    if len(strikes) > 1:
        width = np.min(np.diff(strikes)) * 0.4
    else:
        width = 10
    
    # Calculate a pinyin name for titles and filenames
    base_name = os.path.basename(csv_path).replace('.csv', '')
    pinyin_name = base_name.replace('沪深300', 'hs300').replace('中证1000', 'zz1000').replace('中证500', 'zz500').replace('上证50', 'sz50').replace('深证100', 'sz100').replace('股指期权', '')

    # Plot 1: Volume
    axes[0].bar(calls['strike'] - width/2, calls['volume'], width=width, color='#1f77b4', label='Call Volume', alpha=0.8)
    axes[0].bar(puts['strike'] + width/2, puts['volume'], width=width, color='#ff7f0e', label='Put Volume', alpha=0.8)
    axes[0].set_title(f'Liquidity Distribution: Volume - {pinyin_name}')
    axes[0].set_ylabel('Volume')
    axes[0].legend()
    axes[0].grid(axis='y', linestyle='--', alpha=0.7)
    
    # Plot 2: Open Interest (position)
    axes[1].bar(calls['strike'] - width/2, calls['position'], width=width, color='#1f77b4', label='Call Open Interest', alpha=0.8)
    axes[1].bar(puts['strike'] + width/2, puts['position'], width=width, color='#ff7f0e', label='Put Open Interest', alpha=0.8)
    axes[1].set_title('Liquidity Distribution: Open Interest (Position)')
    axes[1].set_ylabel('Open Interest')
    axes[1].legend()
    axes[1].grid(axis='y', linestyle='--', alpha=0.7)

    # Plot 3: Bid-Ask Spread (if available)
    if 'sprice' in df.columns and 'bprice' in df.columns:
        mid_price = (df['sprice'] + df['bprice']) / 2
        df['spread'] = np.where(mid_price > 0, (df['sprice'] - df['bprice']) / mid_price * 100, 0)
        calls_s = df[df['type'] == 'C'].sort_values('strike')
        puts_s = df[df['type'] == 'P'].sort_values('strike')
        
        axes[2].plot(calls_s['strike'], calls_s['spread'], marker='o', color='#1f77b4', label='Call Spread (%)')
        axes[2].plot(puts_s['strike'], puts_s['spread'], marker='s', color='#ff7f0e', label='Put Spread (%)')
        axes[2].set_title('Bid-Ask Spread Distribution (% of Mid Price)')
        axes[2].set_ylabel('Spread (%)')
        axes[2].legend()
        axes[2].grid(True, linestyle='--', alpha=0.7)
    else:
        axes[2].set_visible(False)

    # Plot 4: Implied Volatility
    if 'future_price' in df.columns and 'days_to_expire' in df.columns:
        S = df['future_price'].iloc[0] if 'future_price' in df.columns else df['spot_price'].iloc[0]
        T = df['days_to_expire'].iloc[0] / 365.0
        r = 0.02 # Assuming 2% rate
        
        df['iv'] = df.apply(lambda row: find_iv((row['bprice'] + row['sprice'])/2, S, row['strike'], T, r, row['type']), axis=1)
        calls_iv = df[df['type'] == 'C'].sort_values('strike')
        puts_iv = df[df['type'] == 'P'].sort_values('strike')
        
        axes[3].plot(calls_iv['strike'], calls_iv['iv'] * 100, marker='o', color='#1f77b4', label='Call IV', alpha=0.8)
        axes[3].plot(puts_iv['strike'], puts_iv['iv'] * 100, marker='s', color='#ff7f0e', label='Put IV', alpha=0.8)
        axes[3].set_title('Implied Volatility (IV) Smile')
        axes[3].set_ylabel('IV %')
        axes[3].legend()
        axes[3].grid(True, linestyle='--', alpha=0.7)
    else:
        axes[3].set_visible(False)
        
    # Set x-axis ticks to 100-point intervals
    min_strike = np.floor(df['strike'].min() / 100) * 100
    max_strike = np.ceil(df['strike'].max() / 100) * 100
    axes[-1].set_xticks(np.arange(min_strike, max_strike + 1, 100))
    
    # Add vertical line for spot price if present
    if 'spot_price' in df.columns and not df['spot_price'].isna().all():
        spot_price = df['spot_price'].iloc[0]
        for ax in axes:
            ax.axvline(x=spot_price, color='red', linestyle=':', linewidth=2, label='Spot Price')

    # Add vertical line for future price if present
    if 'future_price' in df.columns and not df['future_price'].isna().all():
        future_price = df['future_price'].iloc[0]
        for ax in axes:
            ax.axvline(x=future_price, color='green', linestyle=':', linewidth=2, label='Future Price')

    # Deduplicate labels
    for ax in axes:
        handles, labels = ax.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        if by_label:
            ax.legend(by_label.values(), by_label.keys())

    plt.tight_layout()
    
    # Save
    output_dir = 'plots'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f'{pinyin_name}_liquidity.png')
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"Saved plot directly to {output_path}")

def main():
    if len(sys.argv) > 1:
        path = sys.argv[1]
        if os.path.isdir(path):
            files = glob.glob(os.path.join(path, '**', '*.csv'), recursive=True)
        else:
            files = [path]
    else:
        # Default to processing the active file or all files as demonstration
        # To avoid being overwhelming, we can just process a specific active file if it exists,
        # otherwise process the option_data dir
        files = glob.glob('option_data/**/*.csv', recursive=True)
    
    if not files:
        print("No CSV files found.")
        return
    
    print(f"Found {len(files)} files to process.")
    for f in files:
        plot_liquidity(f)

if __name__ == '__main__':
    main()
