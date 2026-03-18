import os
import glob
import pandas as pd
import numpy as np
import re
from datetime import datetime

# Configuration
DATA_DIR = 'option_data'
START_DATE = '20260306'
CASH_YIELD = 2.0  # Annualized yield for holding cash in %
FEE_PER_CONTRACT = 0 # Transaction cost in index points
INITIAL_CAPITAL = 10000.0
SWITCH_THRESHOLD = 5.0 # Total alpha gain required to switch (in index points * qty)

def get_box_spread_candidates(df):
    if df.empty:
        return {}
    
    # Use dictionary for O(1) lookup
    df_c = df[df['type'] == 'C'].set_index('strike').to_dict('index')
    df_p = df[df['type'] == 'P'].set_index('strike').to_dict('index')
    
    common_strikes = sorted(set(df_c.keys()) & set(df_p.keys()))
    if len(common_strikes) < 2:
        return {}
    
    # Pre-extract data
    strike_data = []
    for K in common_strikes:
        c = df_c[K]
        p = df_p[K]
        
        c_b, c_s = c['bprice'], c['sprice']
        p_b, p_s = p['bprice'], p['sprice']
        
        if c_b <= 0 or c_s <= 0 or p_b <= 0 or p_s <= 0:
            continue
            
        dte = c['days_to_expire'] if 'days_to_expire' in c else 0
        
        # Prices: Bid is min, Ask is max
        c_ask, c_bid = max(c_b, c_s), min(c_b, c_s)
        p_ask, p_bid = max(p_b, p_s), min(p_b, p_s)
        
        strike_data.append({
            'K': K,
            'c_ask': c_ask, 'c_bid': c_bid,
            'p_ask': p_ask, 'p_bid': p_bid,
            'dte': dte
        })
    
    results = {}
    for i, s1 in enumerate(strike_data):
        K1 = s1['K']
        for s2 in strike_data[i+1:]:
            K2 = s2['K']
            # Long box: buy call K1, sell call K2, buy put K2, sell put K1
            # Cost = Ask(C1) - Bid(C2) + Ask(P2) - Bid(P1)
            box_buy = (s1['c_ask'] - s2['c_bid']) + (s2['p_ask'] - s1['p_bid'])
            
            # To exit: sell call K1, buy call K2, sell put K2, buy put K1
            # Credit = Bid(C1) - Ask(C2) + Bid(P2) - Ask(P1)
            box_sell = (s1['c_bid'] - s2['c_ask']) + (s2['p_bid'] - s1['p_ask'])
            
            # Mid price for valuation
            box_mid = (s1['c_ask'] + s1['c_bid'] - (s2['c_ask'] + s2['c_bid']) + 
                       s2['p_ask'] + s2['p_bid'] - (s1['p_ask'] + s1['p_bid'])) / 2
            
            width = K2 - K1
            dte = s1['dte']
            
            results[(K1, K2)] = {
                'K1': K1,
                'K2': K2,
                'box_buy': box_buy,
                'box_sell': box_sell,
                'box_mid': box_mid,
                'width': width,
                'dte': dte
            }
    return results

def calculate_alpha(price, width, dte, yield_rate):
    """
    Expected excess gain at maturity relative to holding cash.
    """
    if dte <= 0: return -1000.0
    t = dte / 365.0
    return width - price * (1 + (yield_rate / 100.0) * t)

def get_ticker_from_filename(f):
    bn = os.path.basename(f)
    m = re.search(r'([A-Z]{2}\d{4})', bn)
    if m: return m.group(1)
    
    if '上证50' in bn: t_pre = 'HO'
    elif '沪深300' in bn: t_pre = 'IO'
    elif '中证1000' in bn: t_pre = 'MO'
    else: t_pre = 'XX'
    
    m_exp = re.search(r'_(\d{6})_', bn)
    if m_exp:
        return t_pre + m_exp.group(1)[2:]
    return t_pre + "0000"

def get_sort_key_for_ticker(ticker):
    idx_map = {'HO': 0, 'IO': 1, 'MO': 2}
    pre = ticker[:2]
    exp = ticker[2:]
    return (idx_map.get(pre, 3), exp)

def main():
    dates = sorted([d for d in os.listdir(DATA_DIR) if d.startswith('2026') and not d.startswith('bad')])
    dates = [d for d in dates if d >= START_DATE]
    
    # Pre-scan for all tickers to establish stable Portfolio IDs
    all_tickers_set = set()
    for date in dates:
        pattern = os.path.join(DATA_DIR, date, "*.csv")
        files = glob.glob(pattern)
        for f in files:
            bn = os.path.basename(f)
            if any(idx_name in bn for idx_name in ['上证50股指期权', '沪深300股指期权', '中证1000股指期权']):
                if bn.startswith('stats_'): continue
                all_tickers_set.add(get_ticker_from_filename(f))
    
    sorted_tickers = sorted(list(all_tickers_set), key=get_sort_key_for_ticker)
    ticker_to_pid = {ticker: i + 1 for i, ticker in enumerate(sorted_tickers)}
    pid_to_ticker = {i + 1: ticker for i, ticker in enumerate(sorted_tickers)}
    
    # Initialize portfolios: one per unique ticker
    portfolios = {pid: {'cash': INITIAL_CAPITAL, 'long_pos': None, 'last_date': None, 'expected_gain': 0.0} 
                  for pid in pid_to_ticker.keys()}
    
    all_results = []
    
    print(f"Box Spread Replicator v3.2 - Long Only (Stable Ticker-to-Portfolio Mapping)")
    print(f"Start: {START_DATE} | Initial Capital: {INITIAL_CAPITAL} | Yield: {CASH_YIELD}%")
    print("-" * 80)
    
    for date in dates:
        dt_curr = datetime.strptime(date, '%Y%m%d')
        
        # Load all data for this day
        pattern = os.path.join(DATA_DIR, date, "*.csv")
        files = glob.glob(pattern)
        day_data = {} # ticker -> {'candidates': ..., 'dte': ...}
        
        for f in files:
            bn = os.path.basename(f)
            if any(idx_name in bn for idx_name in ['上证50股指期权', '沪深300股指期权', '中证1000股指期权']):
                if bn.startswith('stats_'): continue
                ticker = get_ticker_from_filename(f)
                df = pd.read_csv(f)
                candidates = get_box_spread_candidates(df)
                
                # Get DTE even if no candidates
                dte = 0
                if not df.empty and 'days_to_expire' in df.columns:
                    dte = df['days_to_expire'].iloc[0]
                elif candidates:
                    # fallback to candidates if available
                    dte = next(iter(candidates.values()))['dte']
                
                day_data[ticker] = {'candidates': candidates, 'dte': dte}

        # Process each portfolio
        for pid in sorted(portfolios.keys()):
            p = portfolios[pid]
            ticker = pid_to_ticker[pid]
            
            data = day_data.get(ticker, {'candidates': {}, 'dte': 0})
            candidates = data['candidates']
            curr_day_dte = data['dte']
            
            # 1. Cash Interest
            if p['last_date']:
                dt_prev = datetime.strptime(p['last_date'], '%Y%m%d')
                days = (dt_curr - dt_prev).days
                if days > 0:
                    p['cash'] += p['cash'] * (CASH_YIELD / 100.0) * (days / 365.0)
            p['last_date'] = date
            
            # 2. Strategy Detection
            best_long, best_long_alpha = None, -1000.0
            for k, v in candidates.items():
                l_alpha = calculate_alpha(v['box_buy'] + 4 * FEE_PER_CONTRACT, v['width'], v['dte'], CASH_YIELD)
                if l_alpha > best_long_alpha:
                    best_long_alpha, best_long = l_alpha, k
            
            action = "HOLD"
            # 3. Settlement
            if p['long_pos']:
                kl, kr, qty = p['long_pos']
                # If ticker data missing or dte too low, settle
                if (kl, kr) not in candidates or curr_day_dte <= 1:
                    w = candidates.get((kl, kr), {'width': kr-kl})['width']
                    p['cash'] += qty * w
                    p['long_pos'] = None
                    action = "SETTLE"

            # 4. Trading Logic
            # LONG TRADE
            if best_long:
                vals_l = candidates[best_long]
                if p['long_pos'] is None:
                    if best_long_alpha > 0.05:
                        cost = (vals_l['box_buy'] + 4 * FEE_PER_CONTRACT)
                        if p['cash'] > 0:
                            qty_l = p['cash'] / cost
                            p['cash'] -= qty_l * cost
                            p['long_pos'] = (best_long[0], best_long[1], qty_l)
                            p['expected_gain'] += best_long_alpha * qty_l
                            action = "OPEN LONG" if action == "HOLD" else action + " / OPEN LONG"
                else:
                    curr_k = (p['long_pos'][0], p['long_pos'][1])
                    v_curr = candidates.get(curr_k)
                    if v_curr:
                        alpha_hold = (v_curr['width'] - (v_curr['box_sell'] - 4 * FEE_PER_CONTRACT) * (1 + (CASH_YIELD/100.0) * (v_curr['dte']/365.0))) * p['long_pos'][2]
                        cash_after_exit = p['cash'] + p['long_pos'][2] * (v_curr['box_sell'] - 4 * FEE_PER_CONTRACT)
                        if cash_after_exit > 0:
                            qty_new = cash_after_exit / (vals_l['box_buy'] + 4 * FEE_PER_CONTRACT)
                            alpha_switch = best_long_alpha * qty_new
                            if alpha_switch > alpha_hold + SWITCH_THRESHOLD:
                                p['cash'] = cash_after_exit
                                cost_new = (vals_l['box_buy'] + 4 * FEE_PER_CONTRACT)
                                qty_l = p['cash'] / cost_new
                                p['cash'] -= qty_l * cost_new
                                p['long_pos'] = (best_long[0], best_long[1], qty_l)
                                p['expected_gain'] += (alpha_switch - alpha_hold)
                                action = "SWITCH LONG" if action == "HOLD" else action + " / SWITCH LONG"

            # 5. Reporting Metrics
            nav = p['cash']
            holding_str, width, ann_yield = "CASH", 0, 0
            # Use curr_day_dte for reporting even if CASH
            dte = curr_day_dte
            
            if p['long_pos']:
                kl, kr, qty_l = p['long_pos']
                v = candidates.get((kl, kr), {'box_sell': kr-kl, 'box_buy': kr-kl, 'dte': dte, 'width': kr-kl})
                nav += qty_l * v['box_sell']
                width = v['width']
                holding_str = f"Long {kl}-{kr} ({qty_l:.2f})"
                ann_yield = -np.log(v['box_buy']/width) / (dte/365.0) * 100 if dte > 0 and v['box_buy'] > 0 else 0
            
            # Only report if ticker was active on this day (curr_day_dte > 0) or if we have a position
            if curr_day_dte > 0 or p['long_pos']:
                all_results.append({
                    'Date': date, 'PortfolioId': pid, 'Ticker': ticker, 'Pos': holding_str,
                    'DTE': dte, 'Width': width, 'ExpNetGainMaturity': round(max(0, p['expected_gain']), 2),
                    'AnnYield%': round(ann_yield, 2), 'Cash': round(p['cash'], 2),
                    'Action': action, 'NAV': round(nav, 2)
                })
        print(f"Processed {date}")

    df_final = pd.DataFrame(all_results)
    df_final = df_final.sort_values(by=['PortfolioId', 'Date'])
    df_final.to_csv('boxx_results_v3.csv', index=False)
    print(f"\nResults saved to boxx_results_v3.csv")

if __name__ == "__main__":
    main()
