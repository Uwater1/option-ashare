import pandas as pd
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
import sys
import os
import glob
import math

# Constants
R = 0.02 # Risk-free rate (approximate)

def black_scholes_price(S, K, T, r, sigma, option_type='C'):
    if T <= 0:
        return max(0, S - K) if option_type == 'C' else max(0, K - S)
    if sigma <= 0:
        return max(0, S - K) if option_type == 'C' else max(0, K - S)
        
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    if option_type == 'C':
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    else:
        return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def find_iv(market_price, S, K, T, r, option_type='C'):
    intrinsic = max(0, S - K) if option_type == 'C' else max(0, K - S)
    if market_price <= intrinsic * 0.99:
        return np.nan
        
    try:
        def objective(sigma):
            return black_scholes_price(S, K, T, r, sigma, option_type) - market_price
        
        # Check if 0.01% or 1000% vol brackets the solution
        if objective(1e-4) * objective(10.0) > 0:
            return np.nan
            
        return brentq(objective, 1e-4, 10.0)
    except:
        return np.nan

def black_scholes_greeks(S, K, T, r, sigma, option_type='C'):
    """
    Calculate Greeks: Delta, Gamma, Theta, Vega, and Probability of Exercise (N(d2))
    Vega is returned per 1% change in volatility (standard convention).
    Theta is returned as per-day value.
    ProbEx is N(d2) for calls and N(-d2) for puts.
    """
    if T <= 0 or pd.isna(sigma) or sigma <= 0:
        # For zero-time or zero-vol, define limits
        if T <= 0:
            if option_type == 'C':
                return (1.0 if S > K else 0.0), 0.0, 0.0, 0.0, (1.0 if S > K else 0.0)
            else:
                return (-1.0 if S < K else 0.0), 0.0, 0.0, 0.0, (1.0 if S < K else 0.0)
        return 0.0, 0.0, 0.0, 0.0, 0.0
    
    sqrtT = np.sqrt(T)
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    
    pdf_d1 = norm.pdf(d1)
    
    if option_type == 'C':
        delta = norm.cdf(d1)
        prob_ex = norm.cdf(d2)
        # Standard Theta formula for call
        theta = (- (S * pdf_d1 * sigma) / (2 * sqrtT) 
                 - r * K * np.exp(-r * T) * norm.cdf(d2))
    else:
        delta = norm.cdf(d1) - 1
        prob_ex = norm.cdf(-d2)
        # Standard Theta formula for put
        theta = (- (S * pdf_d1 * sigma) / (2 * sqrtT) 
                 + r * K * np.exp(-r * T) * norm.cdf(-d2))
        
    gamma = pdf_d1 / (S * sigma * sqrtT)
    # Vega (standard convention: per 1% change in sigma)
    vega = S * pdf_d1 * sqrtT / 100 
    # Theta per day
    theta_day = theta / 365
    
    return delta, gamma, theta_day, vega, prob_ex

def display_option_chain(df):
    """
    Processes and displays the option data in a formatted T-table form.
    """
    # Filter out redundant columns if any, and handle potential naming variations
    df.columns = df.columns.str.strip()
    
    # Required columns for calculation
    required = {'type', 'strike', 'bprice', 'sprice', 'days_to_expire'}
    if not required.issubset(df.columns):
        print(f"[!] Warning: Data missing some required columns: {required - set(df.columns)}")
        # Check if sprice/bprice are present under different names
        # In some akshare versions they are different
        return

    # Determine Underlying Price (Future preferred for index options parity)
    future_p = df['future_price'].iloc[0] if 'future_price' in df.columns else np.nan
    spot_p = df['spot_price'].iloc[0] if 'spot_price' in df.columns else np.nan
    
    # S is the price used for calculations (prefer future_price if available, else spot)
    S = future_p if not pd.isna(future_p) else spot_p
    
    if pd.isna(S):
        print("[!] No spot_price or future_price found in data.")
        return

    dte = df['days_to_expire'].iloc[0]
    T = dte / 365.0
    r = R

    # Pre-process: Mid Price and implied vol
    df['mid'] = (df['bprice'] + df['sprice']) / 2
    # Ensure mid price is valid
    df = df[df['mid'] > 0].copy()
    
    # Calculate IV
    df['iv'] = df.apply(lambda row: find_iv(row['mid'], S, row['strike'], T, r, row['type']), axis=1)
    
    # Greeks
    greek_vals = df.apply(lambda row: black_scholes_greeks(S, row['strike'], T, r, row['iv'], row['type']), axis=1)
    df[['delta', 'gamma', 'theta', 'vega', 'prob_ex']] = pd.DataFrame(greek_vals.tolist(), index=df.index)

    # Pivot into Call and Put
    calls = df[df['type'] == 'C'].copy().set_index('strike')
    puts = df[df['type'] == 'P'].copy().set_index('strike')
    
    all_strikes = sorted(df['strike'].unique())
    
    # Find two strikes closest to spot_price
    ref_price = spot_p if not pd.isna(spot_p) else S
    closest_strikes = sorted(all_strikes, key=lambda x: abs(x - ref_price))[:2]

    # Build Header
    ticker = df['ticker'].iloc[0] if 'ticker' in df.columns else "Unknown"
    print("\n" + "="*138)
    print(f" OPTION CHAIN: {ticker:^15} | Spot: {spot_p:>8.2f} | Future: {future_p:>8.2f} | DTE: {dte:>3} | Rate: {r*100:>4.1f}% ")
    print("="*138)
    
    col_width = 7
    # Group Labels: 8 columns on each side now (IV, Prob, Delta, Gamma, Theta, Vega, Bid, Ask)
    # Total separator pipes on each side: 7
    call_group_label = "CALL (Greeks & Prices)".center(col_width * 8 + 7)
    put_group_label = "PUT (Prices & Greeks)".center(col_width * 8 + 7)
    print(f"{call_group_label} | {' '*col_width} | {put_group_label}")
    
    # Header format
    headers = [
        "IV %", "Prob", "Delta", "Gamma", "Theta", "Vega", "Bid", "Ask", 
        "STRIKE", 
        "Bid", "Ask", "IV %", "Prob", "Delta", "Gamma", "Theta", "Vega"
    ]
    
    def pad(s, w=col_width): return str(s).center(w)
    
    h_str = "|".join([pad(h) for h in headers[:8]]) + " | " + pad(headers[8]) + " | " + "|".join([pad(h) for h in headers[9:]])
    print(h_str)
    print("-" * len(h_str))

    for K in all_strikes:
        c = calls.loc[K] if K in calls.index else None
        p = puts.loc[K] if K in puts.index else None
        
        def f(val, fmt=".3f"):
            if pd.isna(val) or val is None: return "-".center(col_width)
            if isinstance(val, str): return val.center(col_width)
            return f"{val:{fmt}}".center(col_width)

        def f_price(val):
            if pd.isna(val) or val is None: return "-".center(col_width)
            return f"{val:>.1f}".center(col_width)

        def f_iv(val):
            if pd.isna(val) or val is None: return "-".center(col_width)
            return f"{val*100:>.1f}".center(col_width)

        # Call data
        c_vals = [
            f_iv(c['iv']) if c is not None else f(None),
            f(c['prob_ex']) if c is not None else f(None),
            f(c['delta']) if c is not None else f(None),
            f(c['gamma']) if c is not None else f(None),
            f(c['theta']) if c is not None else f(None),
            f(c['vega']) if c is not None else f(None),
            f_price(c['bprice']) if c is not None else f(None),
            f_price(c['sprice']) if c is not None else f(None)
        ]
        
        # Put data
        p_vals = [
            f_price(p['bprice']) if p is not None else f(None),
            f_price(p['sprice']) if p is not None else f(None),
            f_iv(p['iv']) if p is not None else f(None),
            f(p['prob_ex']) if p is not None else f(None),
            f(p['delta']) if p is not None else f(None),
            f(p['gamma']) if p is not None else f(None),
            f(p['theta']) if p is not None else f(None),
            f(p['vega']) if p is not None else f(None)
        ]
        
        # Format strike label with asterisks if it's one of the closest to spot
        strike_label = str(int(K))
        if K in closest_strikes:
            strike_label = f"*{strike_label}*"
            
        row_str = "|".join(c_vals) + " | " + pad(strike_label) + " | " + "|".join(p_vals)
        print(row_str)

    print("-" * len(h_str))
    print(f"Notes: IV % is Implied Vol. Prob is prob. of exercise. Greeks via BS. Theta/day. Vega/1% vol.")
    print("="*138 + "\n")

def main():
    if len(sys.argv) < 2:
        print("Usage: python display.py <path_to_option_csv>")
        # Try to find a sample file to display if no arg passed
        available_files = glob.glob("option_data/202*/*.csv")
        if available_files:
            target = sorted(available_files, key=os.path.getmtime, reverse=True)[0]
            print(f"No file specified. Using latest found: {target}")
            df = pd.read_csv(target)
            display_option_chain(df)
        else:
            print("No data files found in option_data/")
        return

    path = sys.argv[1]
    if os.path.isfile(path):
        df = pd.read_csv(path)
        display_option_chain(df)
    elif os.path.isdir(path):
        files = sorted(glob.glob(os.path.join(path, "*.csv")))
        for f in files:
            print(f"\nProcessing {os.path.basename(f)}...")
            df = pd.read_csv(f)
            display_option_chain(df)
    else:
        print(f"Error: Path {path} not found.")

if __name__ == "__main__":
    main()
