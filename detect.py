import os
import glob
import pandas as pd
import argparse
from typing import List, Dict

MIN_ANNUALIZED_RETURN = 0.05
BORROW_RATE = 0.08 #Cost of borrowing stock

def calculate_annualized_return(profit: float, capital: float, days_to_expire: int) -> float:
    if capital <= 0:
        return float('inf')
    if days_to_expire == 0:
        days_to_expire = 1
    ret = profit / capital
    return ret * (365.0 / days_to_expire)

def get_buy_price(row):
    # Conservative: we pay the ask price (sprice)
    p = float(row['sprice'])
    return p if p > 0 else float('inf')

def get_sell_price(row):
    # Conservative: we receive the bid price (bprice)
    p = float(row['bprice'])
    return p if p > 0 else 0.0

def get_underlying(ticker: str) -> str:
    # HO: SSE 50, IO: CSI 300, MO: CSI 1000
    if not isinstance(ticker, str): return "Unknown"
    for prefix in ['HO', 'IO', 'MO']:
        if ticker.startswith(prefix):
            return prefix
    return ticker[:2]

def get_margin(spot_price, underlying='HO'):
    # Simple margin estimate: 10% of spot
    return 0.1 * spot_price

def detect_put_call_parity(df: pd.DataFrame, results: List[Dict]):
    groups = df.groupby(['underlying', 'strike', 'days_to_expire'])
    for (und, strike, dte), group in groups:
        if dte <= 0: continue
        calls = group[group['type'] == 'C']
        puts = group[group['type'] == 'P']
        if calls.empty or puts.empty: continue
        call = calls.iloc[0]
        put = puts.iloc[0]
        if call['bprice'] <= 0 or call['sprice'] <= 0 or put['bprice'] <= 0 or put['sprice'] <= 0: continue
        S = call['spot_price']
        K = strike
        margin = get_margin(S)
        # Conversion
        cost_conv = S + get_buy_price(put) - get_sell_price(call)
        profit_conv = K - cost_conv
        if profit_conv > 1.0: # Filter small noise
            ann_ret = calculate_annualized_return(profit_conv, margin, dte)
            if ann_ret >= MIN_ANNUALIZED_RETURN:
                results.append({
                    'Strategy': 'Conversion', 
                    'Cost': round(cost_conv, 2),
                    'BorrowCost': 0,
                    'Details': f"K:{K} DTE:{dte} | BuyStock:{S:.1f} + BuyPut@{get_buy_price(put):.2f} - SellCall@{get_sell_price(call):.2f}", 
                    'Profit': round(profit_conv, 2), 'Margin': round(margin, 2), 'Ann. Return': ann_ret, 'underlying': und
                })
        # Reversal
        borrow_cost = S * BORROW_RATE * (dte / 365.0)
        cost_rev = S + get_sell_price(put) - get_buy_price(call)
        profit_rev = (cost_rev - K) - borrow_cost
        if profit_rev > 1.0: # Filter small noise
            ann_ret = calculate_annualized_return(profit_rev, margin, dte)
            if ann_ret >= MIN_ANNUALIZED_RETURN:
                results.append({
                    'Strategy': 'Reversal', 
                    'Cost': round(cost_rev, 2),
                    'BorrowCost': round(borrow_cost, 2),
                    'Details': f"K:{K} DTE:{dte} | SellStock:{S:.1f} + SellPut@{get_sell_price(put):.2f} - BuyCall@{get_buy_price(call):.2f}", 
                    'Profit': round(profit_rev, 2), 'Margin': round(margin, 2), 'Ann. Return': ann_ret, 'underlying': und
                })

def detect_box_spreads(df: pd.DataFrame, results: List[Dict]):
    groups_by_und_dte = df.groupby(['underlying', 'days_to_expire'])
    for (und, dte), group in groups_by_und_dte:
        if dte <= 0: continue
        strikes = sorted(group['strike'].unique())
        strike_data = {}
        for K in strikes:
            k_data = group[group['strike'] == K]
            calls = k_data[k_data['type'] == 'C']
            puts = k_data[k_data['type'] == 'P']
            if not calls.empty and not puts.empty:
                c = calls.iloc[0]; p = puts.iloc[0]
                if c['bprice'] > 0 and c['sprice'] > 0 and p['bprice'] > 0 and p['sprice'] > 0:
                    strike_data[K] = {'C_buy': get_buy_price(c), 'C_sell': get_sell_price(c), 'P_buy': get_buy_price(p), 'P_sell': get_sell_price(p), 'spot': c['spot_price']}
        valid_strikes = sorted(strike_data.keys())
        for i in range(len(valid_strikes)):
            for j in range(i+1, len(valid_strikes)):
                K1 = valid_strikes[i]; K2 = valid_strikes[j]
                sd1 = strike_data[K1]; sd2 = strike_data[K2]
                S = sd1['spot']; margin = get_margin(S)
                # Long Box
                debit = sd1['C_buy'] - sd2['C_sell'] + sd2['P_buy'] - sd1['P_sell']
                profit = (K2 - K1) - debit
                if profit > 1.0:
                    ann_ret = calculate_annualized_return(profit, margin, dte)
                    if ann_ret >= MIN_ANNUALIZED_RETURN:
                        results.append({
                            'Strategy': 'Long Box', 
                            'Cost': round(debit, 2),
                            'BorrowCost': 0,
                            'Details': f"K1:{K1} K2:{K2} DTE:{dte} | C:{sd1['C_buy']:.1f}-{sd2['C_sell']:.1f} P:{sd2['P_buy']:.1f}-{sd1['P_sell']:.1f}", 
                            'Profit': round(profit, 2), 'Margin': round(margin, 2), 'Ann. Return': ann_ret, 'underlying': und
                        })
                # Short Box
                credit = sd1['C_sell'] - sd2['C_buy'] + sd2['P_sell'] - sd1['P_buy']
                profit = credit - (K2 - K1)
                if profit > 1.0:
                    ann_ret = calculate_annualized_return(profit, margin, dte)
                    if ann_ret >= MIN_ANNUALIZED_RETURN:
                        results.append({
                            'Strategy': 'Short Box', 
                            'Cost': round(-credit, 2),
                            'BorrowCost': 0,
                            'Details': f"K1:{K1} K2:{K2} DTE:{dte} | Credit:{credit:.2f} StrikeWidth:{K2-K1}", 
                            'Profit': round(profit, 2), 'Margin': round(margin, 2), 'Ann. Return': ann_ret, 'underlying': und
                        })

def detect_vertical_spreads(df: pd.DataFrame, results: List[Dict]):
    groups_by_und_dte = df.groupby(['underlying', 'days_to_expire'])
    for (und, dte), group in groups_by_und_dte:
        if dte <= 0: continue
        strikes = sorted(group['strike'].unique())
        strike_data = {}
        for K in strikes:
            k_data = group[group['strike'] == K]
            calls = k_data[k_data['type'] == 'C']; puts = k_data[k_data['type'] == 'P']
            strike_data[K] = {}
            if not calls.empty:
                c = calls.iloc[0]
                if c['bprice'] > 0 and c['sprice'] > 0:
                    strike_data[K]['C_buy'] = get_buy_price(c); strike_data[K]['C_sell'] = get_sell_price(c); strike_data[K]['spot'] = c['spot_price']
            if not puts.empty:
                p = puts.iloc[0]
                if p['bprice'] > 0 and p['sprice'] > 0:
                    strike_data[K]['P_buy'] = get_buy_price(p); strike_data[K]['P_sell'] = get_sell_price(p); strike_data[K]['spot'] = p['spot_price']
        valid_strikes = sorted(strike_data.keys())
        for i in range(len(valid_strikes)):
            for j in range(i+1, len(valid_strikes)):
                K1 = valid_strikes[i]; K2 = valid_strikes[j]
                sd1 = strike_data[K1]; sd2 = strike_data[K2]; width = K2 - K1
                S = sd1.get('spot', sd2.get('spot'))
                if S is None: continue
                margin = get_margin(S)
                # Bull Call Monotonicity Violation: Buy K1 Call, Sell K2 Call.
                # True arbitrage only when C(K1_ask) <= C(K2_bid), i.e. cost <= 0.
                # This means we receive a net credit, guaranteed profit regardless of spot at expiry.
                if 'C_buy' in sd1 and 'C_sell' in sd2:
                    cost = sd1['C_buy'] - sd2['C_sell']  # C1_ask - C2_bid
                    if cost < 0:  # Monotonicity violation: low-strike call cheaper than high-strike
                        profit = abs(cost)
                        ann_ret = calculate_annualized_return(profit, profit, dte)  # capital = credit received
                        if ann_ret >= MIN_ANNUALIZED_RETURN:
                            results.append({
                                'Strategy': 'Call Monotonicity', 
                                'Cost': round(cost, 2),
                                'BorrowCost': 0,
                                'Details': f"K1:{K1} K2:{K2} DTE:{dte} | BuyC@{sd1['C_buy']:.2f} SellC@{sd2['C_sell']:.2f} => Credit:{profit:.2f}", 
                                'Profit': round(profit, 2), 'Margin': round(margin, 2), 'Ann. Return': ann_ret, 'underlying': und
                            })
                
                # Bear Call: Sell K1, Buy K2. Credit = C1_sell - C2_buy. Profit = Credit - (K2 - K1)
                if 'C_sell' in sd1 and 'C_buy' in sd2:
                    credit = sd1['C_sell'] - sd2['C_buy']
                    profit = credit - (K2 - K1)
                    if profit > 1.0:
                        ann_ret = calculate_annualized_return(profit, margin, dte)
                        if ann_ret >= MIN_ANNUALIZED_RETURN:
                            results.append({
                                'Strategy': 'Bear Call Arb', 
                                'Cost': round(-credit, 2),
                                'BorrowCost': 0,
                                'Details': f"K1:{K1} K2:{K2} DTE:{dte} | Credit:{credit:.2f}", 
                                'Profit': round(profit, 2), 'Margin': round(margin, 2), 'Ann. Return': ann_ret, 'underlying': und
                            })

                # Bull Put: Sell K2, Buy K1. Credit = P2_sell - P1_buy. Profit = Credit - (K2 - K1)
                if 'P_sell' in sd2 and 'P_buy' in sd1:
                    credit = sd2['P_sell'] - sd1['P_buy']
                    profit = credit - (K2 - K1)
                    if profit > 1.0:
                        ann_ret = calculate_annualized_return(profit, margin, dte)
                        if ann_ret >= MIN_ANNUALIZED_RETURN:
                            results.append({
                                'Strategy': 'Bull Put Arb', 
                                'Cost': round(-credit, 2),
                                'BorrowCost': 0,
                                'Details': f"K1:{K1} K2:{K2} DTE:{dte} | Credit:{credit:.2f}", 
                                'Profit': round(profit, 2), 'Margin': round(margin, 2), 'Ann. Return': ann_ret, 'underlying': und
                            })

def detect_iron_condor_butterfly(df: pd.DataFrame, results: List[Dict]):
    groups_by_und_dte = df.groupby(['underlying', 'days_to_expire'])
    for (und, dte), group in groups_by_und_dte:
        if dte <= 0: continue
        strikes = sorted(group['strike'].unique())
        strike_data = {}
        for K in strikes:
            k_data = group[group['strike'] == K]
            calls = k_data[k_data['type'] == 'C']; puts = k_data[k_data['type'] == 'P']
            strike_data[K] = {}
            if not calls.empty:
                c = calls.iloc[0]
                if c['bprice'] > 0 and c['sprice'] > 0:
                    strike_data[K]['C_buy'] = get_buy_price(c); strike_data[K]['C_sell'] = get_sell_price(c); strike_data[K]['spot'] = c['spot_price']
            if not puts.empty:
                p = puts.iloc[0]
                if p['bprice'] > 0 and p['sprice'] > 0:
                    strike_data[K]['P_buy'] = get_buy_price(p); strike_data[K]['P_sell'] = get_sell_price(p); strike_data[K]['spot'] = p['spot_price']
        valid_strikes = sorted(strike_data.keys())
        for i in range(len(valid_strikes)):
            for k in range(i+2, len(valid_strikes)):
                K1 = valid_strikes[i]; K3 = valid_strikes[k]; K2 = (K1 + K3) / 2.0
                if K2 in strike_data:
                    sd1, sd2, sd3 = strike_data[K1], strike_data[K2], strike_data[K3]
                    S = sd1.get('spot', sd2.get('spot', sd3.get('spot')))
                    if S is None: continue
                    margin = get_margin(S)
                    if 'C_buy' in sd1 and 'C_sell' in sd2 and 'C_buy' in sd3:
                        cost = sd1['C_buy'] - 2 * sd2['C_sell'] + sd3['C_buy']
                        if cost < 0:
                            profit = abs(cost); ann_ret = calculate_annualized_return(profit, margin, dte)
                            if ann_ret >= MIN_ANNUALIZED_RETURN:
                                results.append({
                                    'Strategy': 'Call Butterfly Arb', 
                                    'Cost': round(cost, 2),
                                    'BorrowCost': 0,
                                    'Details': f"K:{K1},{K2},{K3} DTE:{dte} | Credit:{abs(cost):.2f}", 
                                    'Profit': round(profit, 2), 'Margin': round(margin, 2), 'Ann. Return': ann_ret, 'underlying': und
                                })
                    if 'P_buy' in sd1 and 'P_sell' in sd2 and 'P_buy' in sd3:
                        cost = sd1['P_buy'] - 2 * sd2['P_sell'] + sd3['P_buy']
                        if cost < 0:
                            profit = abs(cost); ann_ret = calculate_annualized_return(profit, margin, dte)
                            if ann_ret >= MIN_ANNUALIZED_RETURN:
                                results.append({
                                    'Strategy': 'Put Butterfly Arb', 
                                    'Cost': round(cost, 2),
                                    'BorrowCost': 0,
                                    'Details': f"K:{K1},{K2},{K3} DTE:{dte} | Credit:{abs(cost):.2f}", 
                                    'Profit': round(profit, 2), 'Margin': round(margin, 2), 'Ann. Return': ann_ret, 'underlying': und
                                })

def detect_calendar_arbitrage(df: pd.DataFrame, results: List[Dict]):
    """
    Calendar (K) Arbitrage: Exploiting mispricing between different expiries for same strike.
    Sell near-dated (overpriced), Buy far-dated (underpriced).
    """
    groups = df.groupby(['underlying', 'type', 'strike'])
    for (und, otype, strike), group in groups:
        if len(group) < 2: continue
        sorted_group = group.sort_values('days_to_expire')
        # Compare every pair of expiries
        for i in range(len(sorted_group)):
            for j in range(i+1, len(sorted_group)):
                near = sorted_group.iloc[i]; far = sorted_group.iloc[j]
                if near['bprice'] <= 0 or near['sprice'] <= 0 or far['bprice'] <= 0 or far['sprice'] <= 0: continue
                
                # Sell Near (overpriced), Buy Far (underpriced) — monotonicity violation
                # For European options: longer maturity should always cost >= shorter maturity
                near_sell = get_sell_price(near); far_buy = get_buy_price(far)
                if near_sell > far_buy:
                    profit = near_sell - far_buy
                    # Capital = far_buy (cost of the long leg we must fund)
                    capital = far_buy if far_buy > 0 else 1.0
                    ann_ret = calculate_annualized_return(profit, capital, int(far['days_to_expire']))
                    if ann_ret >= MIN_ANNUALIZED_RETURN:
                        results.append({
                            'Strategy': 'Calendar Arb', 
                            'Cost': round(far_buy - near_sell, 2),
                            'BorrowCost': 0,
                            'Details': f"{otype} K:{strike} | SellNear(DTE:{near['days_to_expire']})@{near_sell:.2f} BuyFar(DTE:{far['days_to_expire']})@{far_buy:.2f}",
                            'Profit': round(profit, 2), 'Margin': round(far_buy, 2), 'Ann. Return': ann_ret, 'underlying': und
                        })

def main():
    parser = argparse.ArgumentParser(description="Detect Option Arbitrage Opportunities")
    parser.add_argument('data_dir', type=str, help='Directory containing option CSV files')
    args = parser.parse_args()
    results = []
    
    # Load all data from files into one DataFrame
    all_data = []
    if os.path.isfile(args.data_dir) and args.data_dir.endswith('.csv'):
        all_data.append(pd.read_csv(args.data_dir))
    else:
        csv_files = glob.glob(os.path.join(args.data_dir, '*.csv'))
        for f in csv_files:
            all_data.append(pd.read_csv(f))
    
    if not all_data:
        print("No CSV data found.")
        return
        
    df = pd.concat(all_data, ignore_index=True)
    df.columns = df.columns.str.strip()
    df['underlying'] = df['ticker'].apply(get_underlying)
    
    # Run all detections on combined data
    detect_put_call_parity(df, results)
    detect_box_spreads(df, results)
    detect_vertical_spreads(df, results)
    detect_iron_condor_butterfly(df, results)
    detect_calendar_arbitrage(df, results) # Added K/Calendar Arb

    print(f"Detected {len(results)} arbitrage opportunities")
    if not results:
        print("No arbitrage opportunities found (Ann. Return >= 0.05).")
        return

    # Group results by underlying
    und_groups = {}
    und_names = {
        'HO': '上证50 (SSE50)',
        'IO': '沪深300 (CSI300)',
        'MO': '中证1000 (CSI1000)'
    }
    
    for res in results:
        und = res.get('underlying', 'Unknown')
        if und not in und_groups:
            und_groups[und] = []
        und_groups[und].append(res)

    all_unds = sorted(df['underlying'].unique())
    for und in all_unds:
        und_full_name = und_names.get(und, und)
        
        if und not in und_groups:
            print(f"\n### {und_full_name}")
            print("No arbitrage opportunities found (Ann. Return >= 5%).")
            continue

        group = und_groups[und]
        group.sort(key=lambda x: x['Profit'], reverse=True)
        
        print(f"\n### {und_full_name} Arbitrage Opportunities (Ann. Return >= 5%, Sorted by Profit)")
        
        header = ["Strategy", "Cost", "Borrow", "Profit", "Margin", "Ann. Return", "Details"]
        col_widths = [12, 8, 8, 8, 8, 11, 70]
        
        def format_row(row):
            ann_ret_str = f"{row['Ann. Return']*100:.2f}%" if isinstance(row['Ann. Return'], float) else str(row['Ann. Return'])
            data = [
                row['Strategy'], 
                row.get('Cost', '-'), 
                row.get('BorrowCost', 0), 
                row['Profit'], 
                row['Margin'], 
                ann_ret_str, 
                row['Details']
            ]
            return "| " + " | ".join(str(data[i]).ljust(col_widths[i]) for i in range(len(header))) + " |"

        print("| " + " | ".join(header[i].ljust(col_widths[i]) for i in range(len(header))) + " |")
        print("|-" + "-|-".join("-" * col_widths[i] for i in range(len(header))) + "-|")
        for res in group:
            print(format_row(res))

if __name__ == "__main__":
    main()
