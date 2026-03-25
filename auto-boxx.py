import sys
import os
import glob
import pandas as pd
import re

def get_ticker_prefix(filename):
    bn = os.path.basename(filename)
    if '上证50' in bn: return 'HO'
    elif '沪深300' in bn: return 'IO'
    elif '中证1000' in bn: return 'MO'
    return 'UNKNOWN'

def main():
    if len(sys.argv) < 2:
        print("Usage: python auto-boxx.py <folder_path>")
        print("Example: python auto-boxx.py option_data/20260323")
        sys.exit(1)
        
    folder_path = sys.argv[1]
    
    # 1. Parse all files
    pattern = os.path.join(folder_path, '*股指期权*.csv')
    files = glob.glob(pattern)
    
    if not files:
        print(f"No option files found in {folder_path} matching *股指期权*.csv")
        sys.exit(1)
        
    all_long_boxes = []
    all_short_boxes = []
    
    for f in files:
        prefix = get_ticker_prefix(f)
        try:
            df = pd.read_csv(f)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            continue
            
        if df.empty: continue
        
        # We need DTE for this file/term
        dte = df['days_to_expire'].iloc[0] if 'days_to_expire' in df.columns else None
        if dte is None or dte <= 0:
            continue
            
        # Get ticker base (e.g. IO2604)
        ticker_base = df['ticker'].iloc[0] if 'ticker' in df.columns else prefix
        
        # Filter out bad prices
        df = df[(df['bprice'] > 0) & (df['sprice'] > 0)]
        
        # Extract C and P
        df_c = df[df['type'] == 'C'].set_index('strike').to_dict('index')
        df_p = df[df['type'] == 'P'].set_index('strike').to_dict('index')
        
        common_strikes = sorted(set(df_c.keys()) & set(df_p.keys()))
        if len(common_strikes) < 2:
            continue
            
        strike_data = []
        for K in common_strikes:
            c = df_c[K]
            p = df_p[K]
            
            c_b, c_s = c['bprice'], c['sprice']
            p_b, p_s = p['bprice'], p['sprice']
            
            # Using actual worst-case execution mathematically:
            # Buy at Ask (max of bid/ask), Sell at Bid (min of bid/ask)
            c_ask, c_bid = max(c_b, c_s), min(c_b, c_s)
            p_ask, p_bid = max(p_b, p_s), min(p_b, p_s)
            
            strike_data.append({
                'K': K,
                'c_ask': c_ask, 'c_bid': c_bid,
                'p_ask': p_ask, 'p_bid': p_bid
            })
            
        # Generate combinations
        for i, s1 in enumerate(strike_data):
            K1 = s1['K']
            for s2 in strike_data[i+1:]:
                K2 = s2['K']
                
                # Long box: Buy C1, Sell C2, Buy P2, Sell P1
                # Cost = C1_ask - C2_bid + P2_ask - P1_bid
                box_buy_cost = (s1['c_ask'] - s2['c_bid']) + (s2['p_ask'] - s1['p_bid'])
                payout = K2 - K1
                long_profit = payout - box_buy_cost
                if box_buy_cost > 0:
                    long_ret = long_profit / box_buy_cost
                    long_ann = long_ret * (365.0 / dte)
                    
                    all_long_boxes.append({
                        'index': prefix,
                        'ticker': ticker_base,
                        'DTE': dte,
                        'K1': K1, 'K2': K2,
                        'c1_ask': s1['c_ask'], 'c2_bid': s2['c_bid'],
                        'p2_ask': s2['p_ask'], 'p1_bid': s1['p_bid'],
                        'cost': box_buy_cost,
                        'profit': long_profit,
                        'ret': long_ret,
                        'ann_ret': long_ann
                    })
                    
                # Short box: Sell C1, Buy C2, Sell P2, Buy P1
                # Credit = C1_bid - C2_ask + P2_bid - P1_ask
                box_sell_credit = (s1['c_bid'] - s2['c_ask']) + (s2['p_bid'] - s1['p_ask'])
                short_profit = box_sell_credit - payout
                margin = payout
                if margin > 0:
                    short_ret = short_profit / margin
                    short_ann = short_ret * (365.0 / dte)
                    
                    all_short_boxes.append({
                        'index': prefix,
                        'ticker': ticker_base,
                        'DTE': dte,
                        'K1': K1, 'K2': K2,
                        'c1_bid': s1['c_bid'], 'c2_ask': s2['c_ask'],
                        'p2_bid': s2['p_bid'], 'p1_ask': s1['p_ask'],
                        'credit': box_sell_credit,
                        'profit': short_profit,
                        'ret': short_ret,
                        'ann_ret': short_ann
                    })

    if not all_long_boxes and not all_short_boxes:
        print("No valid box spread combinations found.")
        return
        
    df_long = pd.DataFrame(all_long_boxes)
    df_short = pd.DataFrame(all_short_boxes)
    
    all_dtes = []
    if not df_long.empty: all_dtes.extend(df_long['DTE'].unique())
    if not df_short.empty: all_dtes.extend(df_short['DTE'].unique())
    
    if not all_dtes:
        print("No valid DTEs found.")
        return
        
    min_dte = min(all_dtes)
    
    print(f"=== Auto-Boxx Analysis for {folder_path} ===")
    print(f"Nearest DTE identified: {min_dte} days\n")
    
    # 1. Long Box Near (DTE == min_dte)
    print("--- 1. Long Box Near (Nearest DTE) Top 5 ---")
    if not df_long.empty:
        df_long_near = df_long[(df_long['DTE'] == min_dte) & (df_long['ret'] >= 0.01)]
        df_long_near = df_long_near.sort_values(by='ann_ret', ascending=False).head(5)
        if not df_long_near.empty:
            for idx, row in df_long_near.iterrows():
                print(f"[{row['index']}] K1: {row['K1']} | K2: {row['K2']} | DTE: {row['DTE']} | Cost: {row['cost']:.2f} | Payout: {row['K2'] - row['K1']:.2f} | Exp Return: {row['ret']*100:.2f}% | Ann Return: {row['ann_ret']*100:.2f}%")
                print(f"Buy {row['ticker']}C{row['K1']}@{row['c1_ask']}; Sell {row['ticker']}C{row['K2']}@{row['c2_bid']}; Buy {row['ticker']}P{row['K2']}@{row['p2_ask']}; Sell {row['ticker']}P{row['K1']}@{row['p1_bid']}")
        else:
            print("No qualifying long boxes found for nearest DTE.")
    else:
        print("No long boxes found.")
    print()
        
    # 2. Long Box Far (min_dte < DTE < 91)
    print("--- 2. Long Box Far (Near term, DTE < 91 but > Nearest) Top 5 ---")
    if not df_long.empty:
        df_long_far = df_long[(df_long['DTE'] > min_dte) & (df_long['DTE'] < 91) & (df_long['ret'] >= 0.01)]
        df_long_far = df_long_far.sort_values(by='ann_ret', ascending=False).head(5)
        if not df_long_far.empty:
            for idx, row in df_long_far.iterrows():
                print(f"[{row['index']}] K1: {row['K1']} | K2: {row['K2']} | DTE: {row['DTE']} | Cost: {row['cost']:.2f} | Payout: {row['K2'] - row['K1']:.2f} | Exp Return: {row['ret']*100:.2f}% | Ann Return: {row['ann_ret']*100:.2f}%")
                print(f"Buy {row['ticker']}C{row['K1']}@{row['c1_ask']}; Sell {row['ticker']}C{row['K2']}@{row['c2_bid']}; Buy {row['ticker']}P{row['K2']}@{row['p2_ask']}; Sell {row['ticker']}P{row['K1']}@{row['p1_bid']}")
        else:
            print("No qualifying long boxes found for far-term DTEs.")
    else:
        print("No long boxes found.")
    print()
        
    # 3. Short Box (DTE < 61)
    print("--- 3. Short Box (All terms DTE < 61) Top 5 ---")
    if not df_short.empty:
        df_short_under_60 = df_short[(df_short['DTE'] < 61) & (df_short['profit'] >= 1.0)]
        df_short_under_60 = df_short_under_60.sort_values(by='ann_ret', ascending=False).head(5)
        if not df_short_under_60.empty:
            for idx, row in df_short_under_60.iterrows():
                print(f"[{row['index']}] K1: {row['K1']} | K2: {row['K2']} | DTE: {row['DTE']} | Credit: {row['credit']:.2f} | Margin: {row['K2'] - row['K1']:.2f} | Exp Return: {row['ret']*100:.2f}% | Ann Return: {row['ann_ret']*100:.2f}%")
                print(f"Sell {row['ticker']}C{row['K1']}@{row['c1_bid']}; Buy {row['ticker']}C{row['K2']}@{row['c2_ask']}; Sell {row['ticker']}P{row['K2']}@{row['p2_bid']}; Buy {row['ticker']}P{row['K1']}@{row['p1_ask']}")
        else:
            print("No qualifying short boxes found for DTE < 61")
    else:
        print("No short boxes found.")
    print()

if __name__ == '__main__':
    main()
