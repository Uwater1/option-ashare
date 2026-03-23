from functools import cache
import pandas as pd
import numpy as np
import lightgbm as lgb
import os
import sys
import json
import re
import math
from numba import njit

# Constants
R = 0.02  # Risk-free rate

@njit(cache=True)
def cdf_jit(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

@njit(cache=True)
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

@njit(cache=True)
def find_iv_jit(market_price, S, K, T, r, is_call):
    intrinsic = max(0.0, S - K) if is_call else max(0.0, K - S)
    if market_price <= intrinsic * 0.99 or market_price <= 0:
        return 0.5
    low, high = 1e-4, 10.0
    p_low = black_scholes_price_jit(S, K, T, r, low, is_call)
    p_high = black_scholes_price_jit(S, K, T, r, high, is_call)
    if (market_price - p_low) * (market_price - p_high) > 0:
        return 0.5
    for _ in range(40):
        mid = (low + high) / 2.0
        if (market_price - black_scholes_price_jit(S, K, T, r, mid, is_call)) > 0:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0

def extract_ticker_prefix(ticker):
    match = re.match(r'([A-Za-z]+)', str(ticker))
    return match.group(1) if match else 'UNK'

def _dte_bucket_code(days_to_expire):
    """Map days_to_expire to integer bucket code matching training order."""
    if days_to_expire <= 7:
        return 0   # 'near'
    elif days_to_expire <= 30:
        return 1   # 'short'
    elif days_to_expire <= 90:
        return 2   # 'medium'
    else:
        return 3   # 'far'

def predict_spread(midprice, ticker, option_type, strike, days_to_expire, future_price):
    if not os.path.exists('spread_model.txt'):
        return None
    model = lgb.Booster(model_file='spread_model.txt')
    with open('model_categories.json', 'r') as f:
        categories = json.load(f)

    ticker_prefix = extract_ticker_prefix(ticker)
    T = days_to_expire / 365.0
    is_call = (option_type == 'C')
    iv = find_iv_jit(midprice, future_price, strike, T, R, is_call)

    ticker_code = categories['tickers'].index(ticker_prefix) if ticker_prefix in categories['tickers'] else -1
    type_code   = categories['types'].index(option_type)     if option_type   in categories['types']   else -1

    log_moneyness = math.log(future_price / strike)
    sqrt_dte      = math.sqrt(T)
    otm_depth     = abs(log_moneyness)
    is_atm        = int(otm_depth < 0.02)
    iv_x_sqrt_dte = iv * sqrt_dte
    dte_bucket    = _dte_bucket_code(days_to_expire)

    data = [[
        midprice,             # midprice
        strike,               # strike
        days_to_expire,       # days_to_expire
        future_price,         # S
        log_moneyness,        # log_moneyness
        iv,                   # iv
        log_moneyness * sqrt_dte,  # log_moneyness_x_sqrt_dte
        sqrt_dte,             # sqrt_dte
        1.0 / (T + 1e-6),    # inv_dte
        otm_depth,            # otm_depth
        iv_x_sqrt_dte,        # iv_x_sqrt_dte
        is_atm,               # is_atm
        ticker_code,          # ticker_cat
        type_code,            # type_cat
        dte_bucket,           # dte_bucket
    ]]

    # Model predicts log(1 + spread); reverse-transform
    log_pred = model.predict(data)[0]
    predicted_spread = math.expm1(log_pred)
    predicted_spread = max(0.0, predicted_spread)

    return predicted_spread, midprice - predicted_spread / 2, midprice + predicted_spread / 2

def main():
    if len(sys.argv) < 7:
        print("Usage: spread.py <midprice> <ticker> <type> <strike> <days_to_expire> <future_price>")
        return
    try:
        mid   = float(sys.argv[1])
        tick  = sys.argv[2]
        typ   = sys.argv[3]
        strik = float(sys.argv[4])
        dte   = float(sys.argv[5])
        fut   = float(sys.argv[6])

        res = predict_spread(mid, tick, typ, strik, dte, fut)
        if res:
            spread, bprice, sprice = res
            print(f" Predicted Spread : {spread:>10.4f}")
            print(f" Bid Price        : {bprice:>10.4f}")
            print(f" Ask Price        : {sprice:>10.4f}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    main()
