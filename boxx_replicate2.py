import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime

# Configuration
DATA_DIR = 'option_data'
START_DATE = '20260306'
CASH_YIELD = 2.0  # Annualized yield for holding cash in %
FEE_PER_CONTRACT = 0.1 # Transaction cost in index points

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
        
        # bprice is bid, sprice is ask based on data format
        c_b, c_s = c['bprice'], c['sprice']
        p_b, p_s = p['bprice'], p['sprice']
        
        if c_b <= 0 or c_s <= 0 or p_b <= 0 or p_s <= 0:
            continue
            
        dte = c['days_to_expire'] if 'days_to_expire' in c else 0
        
        # Prices: Bid is min, Ask is max (to be robust against data anomalies)
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
            
            width = K2 - K1
            dte = s1['dte']
            
            results[(K1, K2)] = {
                'K1': K1,
                'K2': K2,
                'box_buy': box_buy,
                'box_sell': box_sell,
                'width': width,
                'dte': dte
            }
    return results

def calculate_net_gain(box_price, width, dte, yield_rate):
    """
    Expected net gain at maturity relative to holding cash.
    NetGain = Width - Cost * (1 + yield * t)
    """
    t = dte / 365.0
    return width - box_price * (1 + (yield_rate / 100.0) * t)

def main():
    # Find all available dates
    dates = sorted([d for d in os.listdir(DATA_DIR) if d.startswith('202603') and not d.startswith('bad')])
    dates = [d for d in dates if d >= START_DATE]
    
    # Book structure: {ticker: {'pos': (K1, K2) or None, 'cash': float, 'expected_gain': float, 'history': list}}
    books = {}
    
    print(f"Box Spread Replicator 2 - Multi-Book Strategy")
    print(f"Start: {START_DATE} | Cash Yield: {CASH_YIELD}% | Fee: {FEE_PER_CONTRACT}")
    print("-" * 110)
    
    for date in dates:
        # Find all files for this date
        pattern = os.path.join(DATA_DIR, date, f"*_{date}.csv")
        files = glob.glob(pattern)
        
        # Track which tickers we see today
        tickers_today = []
        
        # Prepare daily data
        ticker_candidates = {}
        for f in files:
            if not any(idx_name in os.path.basename(f) for idx_name in ['上证50股指期权', '沪深300股指期权', '中证1000股指期权']):
                continue
            df = pd.read_csv(f)
            if df.empty or 'ticker' not in df.columns:
                continue
            ticker = df['ticker'].iloc[0]
            if not any(ticker.startswith(p) for p in ['HO', 'IO', 'MO']):
                continue
            candidates = get_box_spread_candidates(df)
            if candidates:
                ticker_candidates[ticker] = candidates
                tickers_today.append(ticker)

        # Process each book
        # Fix: Use union of existing books and today's tickers to ensure new tickers are not ignored.
        all_tickers = sorted(set(books.keys()) | set(tickers_today))
        for ticker in all_tickers:
            if ticker not in books:
                books[ticker] = {'pos': None, 'cash': 0.0, 'expected_gain': 0.0, 'history': []}
            
            candidates = ticker_candidates.get(ticker, {})
            action = "HOLD"
            curr_pos = books[ticker]['pos']
            
            # Find the best entry candidate today
            best_key = None
            best_alpha_entry = -np.inf
            for key, vals in candidates.items():
                # Alpha if entered now: Width - (Cost + 4*Fee) * (1 + r*t)
                alpha = calculate_net_gain(vals['box_buy'] + 4 * FEE_PER_CONTRACT, vals['width'], vals['dte'], CASH_YIELD)
                if alpha > best_alpha_entry:
                    best_alpha_entry = alpha
                    best_key = key

            if curr_pos is None:
                # In Cash
                if best_key and best_alpha_entry > 0.01:
                    books[ticker]['pos'] = best_key
                    books[ticker]['cash'] -= (candidates[best_key]['box_buy'] + 4 * FEE_PER_CONTRACT)
                    books[ticker]['expected_gain'] += best_alpha_entry
                    action = f"OPEN {best_key[0]}-{best_key[1]}"
            else:
                # Holding a box
                if curr_pos in candidates:
                    vals_curr = candidates[curr_pos]
                    if vals_curr['dte'] <= 0:
                        # Settle
                        books[ticker]['cash'] += (curr_pos[1] - curr_pos[0])
                        books[ticker]['pos'] = None
                        action = "SETTLE"
                        # After settlement, expected gain remains (it was realized)
                        # We can open a new one if it improves the TOTAL terminal alpha
                        if best_key and best_alpha_entry > 0:
                            # We treat the new entry as adding to the alpha
                            # But user said "expected net gain at maturity"
                            # If we just settled, we have the cash. 
                            # If we open another, we add more alpha.
                            books[ticker]['pos'] = best_key
                            books[ticker]['cash'] -= (candidates[best_key]['box_buy'] + 4 * FEE_PER_CONTRACT)
                            books[ticker]['expected_gain'] += best_alpha_entry
                            action += f" / OPEN {best_key[0]}-{best_key[1]}"
                    else:
                        # Check for switch
                        # Terminal Value difference if we switch:
                        # Fix: Use correct DTE for current and new box to account for timing differences.
                        t_curr = vals_curr['dte'] / 365.0
                        t_new  = candidates[best_key]['dte'] / 365.0
                        extra_alpha = (vals_curr['box_sell'] - 4 * FEE_PER_CONTRACT) * (1 + (CASH_YIELD/100.0) * t_curr) \
                                    - (candidates[best_key]['box_buy'] + 4 * FEE_PER_CONTRACT) * (1 + (CASH_YIELD/100.0) * t_new) \
                                    + candidates[best_key]['width'] - vals_curr['width']
                        
                        if extra_alpha > 0.05: # Threshold for switching
                            books[ticker]['cash'] += (vals_curr['box_sell'] - 4 * FEE_PER_CONTRACT)
                            books[ticker]['pos'] = best_key
                            books[ticker]['cash'] -= (candidates[best_key]['box_buy'] + 4 * FEE_PER_CONTRACT)
                            books[ticker]['expected_gain'] += extra_alpha
                            action = f"SWITCH {curr_pos[0]}-{curr_pos[1]} -> {best_key[0]}-{best_key[1]}"
                else:
                    action = "HOLD (ILLIQ)"
            
            # Record state
            curr_pos = books[ticker]['pos']
            width = 0
            dte = 0
            ann_yield = 0
            if curr_pos and curr_pos in candidates:
                vals = candidates[curr_pos]
                width = vals['width']
                dte = vals['dte']
                ann_yield = -np.log(vals['box_buy']/width) / (dte/365.0) * 100 if dte > 0 and vals['box_buy'] > 0 else 0
            
            books[ticker]['history'].append({
                'Date': date,
                'Ticker': ticker,
                'Pos': f"{curr_pos[0]}-{curr_pos[1]}" if curr_pos else "CASH",
                'DTE': dte,
                'Width': width,
                'ExpNetGainMaturity': round(books[ticker]['expected_gain'], 2),
                'AnnYield%': round(ann_yield, 2),
                'Cash': round(books[ticker]['cash'], 1),
                'Action': action
            })

    # Summary Output
    all_history = []
    for ticker in sorted(books.keys()):
        if books[ticker]['history']:
            all_history.extend(books[ticker]['history'])
    
    df_result = pd.DataFrame(all_history)
    
    print("\n--- Final Status of 12 Books ---")
    last_states = []
    for ticker in sorted(books.keys()):
        if books[ticker]['history']:
            last_states.append(books[ticker]['history'][-1])
    
    df_final = pd.DataFrame(last_states)
    print(df_final.to_string(index=False))
    
    # Optionally save full history to file
    df_result.to_csv('boxx_results_multi.csv', index=False)
    print(f"\nFull history saved to boxx_results_multi.csv")

if __name__ == "__main__":
    main()
