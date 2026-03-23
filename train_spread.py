import pandas as pd
import numpy as np
import lightgbm as lgb
import os
import glob
import re
import json
import math
from numba import njit

# Constants
R = 0.02 # Risk-free rate

@njit
def cdf_jit(x):
    """Fast normal CDF approximation using math.erf"""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

@njit
def black_scholes_price_jit(S, K, T, r, sigma, is_call):
    if T <= 1e-6:
        return max(0.0, S - K) if is_call else max(0.0, K - S)
    if sigma <= 1e-6:
        return max(0.0, S - K) if is_call else max(0.0, K - S)
        
    sqrtT = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    if is_call:
        return S * cdf_jit(d1) - K * math.exp(-r * T) * cdf_jit(d2)
    else:
        return K * math.exp(-r * T) * cdf_jit(-d2) - S * cdf_jit(-d1)

@njit
def find_iv_jit(market_price, S, K, T, r, is_call):
    intrinsic = max(0.0, S - K) if is_call else max(0.0, K - S)
    if market_price <= intrinsic * 0.99 or market_price <= 0:
        return 0.0
        
    low = 1e-4
    high = 10.0
    
    p_low = black_scholes_price_jit(S, K, T, r, low, is_call)
    p_high = black_scholes_price_jit(S, K, T, r, high, is_call)
    
    if (market_price - p_low) * (market_price - p_high) > 0:
        return 0.0
        
    for _ in range(50):
        mid = (low + high) / 2.0
        p_mid = black_scholes_price_jit(S, K, T, r, mid, is_call)
        if (market_price - p_mid) > 0:
            low = mid
        else:
            high = mid
            
    return (low + high) / 2.0

@njit
def calculate_ivs_vec(market_prices, Ss, Ks, Ts, rs, is_calls):
    n = len(market_prices)
    res = np.empty(n)
    for i in range(n):
        res[i] = find_iv_jit(market_prices[i], Ss[i], Ks[i], Ts[i], rs[i], is_calls[i])
    return res

def extract_ticker_prefix(ticker):
    match = re.match(r'([A-Za-z]+)', str(ticker))
    if match:
        return match.group(1)
    return 'UNK'

def load_data(base_dir):
    all_files = glob.glob(os.path.join(base_dir, '202*/*.csv'))
    dfs = []
    for f in all_files:
        try:
            df = pd.read_csv(f)
            required = {'type', 'strike', 'bprice', 'sprice', 'days_to_expire', 'ticker'}
            if not required.issubset(df.columns):
                continue
            dfs.append(df)
        except Exception as e:
            pass
    
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

def preprocess(df):
    for col in ['strike', 'bprice', 'sprice', 'days_to_expire', 'future_price', 'spot_price']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df = df.dropna(subset=['bprice', 'sprice', 'strike', 'days_to_expire']).copy()
    df['spread'] = df['sprice'] - df['bprice']
    df = df[df['spread'] >= 0].copy()
    df['midprice'] = (df['bprice'] + df['sprice']) / 2
    df['ticker_prefix'] = df['ticker'].apply(extract_ticker_prefix)
    df['S'] = df['future_price'].fillna(df['spot_price'])
    df = df.dropna(subset=['S']).copy()
    
    T = df['days_to_expire'].values / 365.0
    is_call = (df['type'] == 'C').values
    
    print("Calculating IV with Numba...")
    df['iv'] = calculate_ivs_vec(
        df['midprice'].values, 
        df['S'].values, 
        df['strike'].values, 
        T, 
        np.full(len(df), R), 
        is_call
    )
    
    iv_median = df.loc[df['iv'] > 0, 'iv'].median()
    df.loc[df['iv'] <= 0, 'iv'] = iv_median
    
    df['log_moneyness'] = np.log(df['S'] / df['strike'])
    df['log_moneyness_x_sqrt_dte'] = df['log_moneyness'] * np.sqrt(T)
    df['sqrt_dte'] = np.sqrt(T)
    df['inv_dte'] = 1.0 / (T + 1e-6)
    
    df['ticker_cat'] = df['ticker_prefix'].astype('category')
    df['type_cat'] = df['type'].astype('category')
    
    return df

def train():
    df = load_data('option_data')
    if df.empty: return
    df = preprocess(df)
    
    features = ['midprice', 'strike', 'days_to_expire', 'S', 
                'log_moneyness', 'iv', 'log_moneyness_x_sqrt_dte', 
                'sqrt_dte', 'inv_dte', 'ticker_cat', 'type_cat']
    
    X = df[features]
    y = df['spread']
    
    model = lgb.LGBMRegressor(
        n_estimators=500,
        learning_rate=0.1,
        num_leaves=31,
        n_jobs=-1
    )
    
    model.fit(X, y)
    model.booster_.save_model('spread_model.txt')
    
    categories = {
        'tickers': df['ticker_prefix'].astype('category').cat.categories.tolist(),
        'types': df['type'].astype('category').cat.categories.tolist()
    }
    with open('model_categories.json', 'w') as f:
        json.dump(categories, f)
    print("Training complete.")

if __name__ == '__main__':
    train()
