import os
import glob
import pandas as pd
import numpy as np

# Configuration
INDICES = {
    'HO': '上证50股指期权',
    'IO': '沪深300股指期权',
    'MO': '中证1000股指期权'
}
EXPIRY = '202603'
DATA_DIR = 'option_data'
START_DATE = '20260306'
FEE_PER_CONTRACT = 0 # Index points per contract (friction)

def get_syn_prices(df):
    """
    Returns a dict: strike -> { 'syn_buy': S_syn, 'syn_sell': S_exit, 'mid': S_mid, 'spot': spot }
    S_syn = K + C_ask - P_bid (Price to enter long)
    S_exit = K + C_bid - P_ask (Price to exit long)
    """
    # Requirement: Ignore pairs with < 5 days to expire
    if 'days_to_expire' in df.columns and df['days_to_expire'].iloc[0] < 5:
        return {}

    results = {}
    calls = df[df['type'] == 'C']
    puts = df[df['type'] == 'P']
    common_strikes = sorted(set(calls['strike']) & set(puts['strike']))
    
    for k in common_strikes:
        c = calls[calls['strike'] == k].iloc[0]
        p = puts[puts['strike'] == k].iloc[0]
        
        if c['bprice'] <= 0 or c['sprice'] <= 0 or p['bprice'] <= 0 or p['sprice'] <= 0:
            continue
            
        # conservative pricing: buy the ask, sell the bid
        c_buy = max(c['bprice'], c['sprice'])
        c_sell = min(c['bprice'], c['sprice'])
        p_buy = max(p['bprice'], p['sprice'])
        p_sell = min(p['bprice'], p['sprice'])
        
        syn_buy = k + c_buy - p_sell
        syn_sell = k + c_sell - p_buy
        syn_mid = k + (c['bprice'] + c['sprice'])/2 - (p['bprice'] + p['sprice'])/2
        
        results[k] = {
            'syn_buy': syn_buy,
            'syn_sell': syn_sell,
            'syn_mid': syn_mid,
            'spot': c['spot_price']
        }
    return results

def main():
    dates = sorted([d for d in os.listdir(DATA_DIR) if d.startswith('202603') and not d.startswith('bad')])
    dates = [d for d in dates if d >= START_DATE]
    
    books = {idx: [] for idx in INDICES}
    positions = {idx: None for idx in INDICES} # {expiry, strike, entry_cost, cum_pnl_offset}
    
    print(f"Delta=1 Keeper - Cross-Expiry & Cross-Strike Optimization")
    print(f"Start: {START_DATE} | Fee: {FEE_PER_CONTRACT}")
    print("-" * 80)

    for date in dates:
        for code, name in INDICES.items():
            # Find all files for this index on this date (multiple expiries)
            pattern = os.path.join(DATA_DIR, date, f"{name}_*_{date}.csv")
            files = glob.glob(pattern)
            if not files: continue
            
            # Global synthetic data for this index on this date
            # Key: (expiry, strike)
            all_syn_data = {}
            for f in files:
                # Extract expiry from filename: {name}_{expiry}_{date}.csv
                # e.g. 上证50股指期权_202603_20260306.csv
                fname = os.path.basename(f)
                parts = fname.replace(".csv", "").split('_')
                if len(parts) < 2: continue
                expiry = parts[-2] 
                
                df = pd.read_csv(f)
                syn_data = get_syn_prices(df)
                for strike, vals in syn_data.items():
                    all_syn_data[(expiry, strike)] = vals
            
            if not all_syn_data: continue
            
            # Find the globally cheapest (expiry, strike) to BUY today
            best_key = min(all_syn_data.keys(), key=lambda k: all_syn_data[k]['syn_buy'])
            best_expiry, best_k = best_key
            best_syn_buy = all_syn_data[best_key]['syn_buy']
            spot = all_syn_data[best_key]['spot']
            
            if positions[code] is None:
                positions[code] = {
                    'expiry': best_expiry,
                    'strike': best_k,
                    'entry_cost': best_syn_buy + 2 * FEE_PER_CONTRACT,
                    'cum_pnl_offset': 0
                }
                action = 'OPEN'
            else:
                curr_expiry = positions[code]['expiry']
                curr_k = positions[code]['strike']
                curr_key = (curr_expiry, curr_k)
                
                # Price to EXIT current position
                curr_syn_sell = all_syn_data[curr_key]['syn_sell'] if curr_key in all_syn_data else -999999
                
                # Logic: Is it cheaper to sell old (any expiry/strike) and buy new (any expiry/strike)?
                if best_key != curr_key and curr_syn_sell > (best_syn_buy + 4 * FEE_PER_CONTRACT):
                    old_desc = f"{curr_expiry} K{curr_k}"
                    # Realize the swap friction/gain into the offset
                    positions[code]['cum_pnl_offset'] += (curr_syn_sell - best_syn_buy - 4 * FEE_PER_CONTRACT)
                    positions[code]['expiry'] = best_expiry
                    positions[code]['strike'] = best_k
                    action = f"SWITCH {old_desc}->{best_expiry} K{best_k}"
                else:
                    action = "HOLD"
            
            # Record state
            active_expiry = positions[code]['expiry']
            active_k = positions[code]['strike']
            active_key = (active_expiry, active_k)
            
            if active_key in all_syn_data:
                active_syn_mid = all_syn_data[active_key]['syn_mid']
            else:
                active_syn_mid = books[code][-1]['SynMid'] if books[code] else 0
                
            pnl = (active_syn_mid - positions[code]['entry_cost']) + positions[code]['cum_pnl_offset']
            advantage = spot - active_syn_mid # Positive means synthetic is cheaper than stock
            
            books[code].append({
                'Date': date,
                'Exp': active_expiry,
                'K': active_k,
                'Spot': round(spot, 1),
                'SynMid': round(active_syn_mid, 1),
                'Adv': round(advantage, 1), # Better than stock by X points
                'BestExp': best_expiry,
                'BestK': best_k,
                'BestSyn': round(all_syn_data[best_key]['syn_mid'], 1),
                'PnL': round(pnl, 1),
                'Action': action
            })

    for code in INDICES:
        print(f"\n--- {code} ({INDICES[code]}) ---")
        df_book = pd.DataFrame(books[code])
        print(df_book.to_string(index=False))

if __name__ == "__main__":
    main()
