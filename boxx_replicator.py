import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime

# Configuration
INDICES = {
    'HO': '上证50股指期权',
    'IO': '沪深300股指期权',
    'MO': '中证1000股指期权'
}
# We consider expiries from 1 to 6 months (approx 30 to 180 days)
MIN_DTE = 20
MAX_DTE = 181
DATA_DIR = 'option_data'
START_DATE = '20260306'
FEE_PER_CONTRACT = 0 # Index points per contract (friction)
CASH_YIELD = 2.0 # Annualized yield for holding cash in %

def get_box_spread_price(df):
    if df.empty:
        return {}
    
    # Requirement: Ignore options with < MIN_DTE or > MAX_DTE days to expire
    if 'days_to_expire' in df.columns:
        # We'll check this per strike/option
        pass
    
    # Use dictionary for O(1) lookup
    df_c = df[df['type'] == 'C'].set_index('strike').to_dict('index')
    df_p = df[df['type'] == 'P'].set_index('strike').to_dict('index')
    
    common_strikes = sorted(set(df_c.keys()) & set(df_p.keys()))
    if len(common_strikes) < 2:
        return {}
    
    # Pre-extract data to avoid repeated dict lookups
    strike_data = []
    for K in common_strikes:
        c = df_c[K]
        p = df_p[K]
        
        c_b, c_s = c['bprice'], c['sprice']
        p_b, p_s = p['bprice'], p['sprice']
        
        if c_b <= 0 or c_s <= 0 or p_b <= 0 or p_s <= 0:
            continue
            
        dte = c['days_to_expire'] if 'days_to_expire' in c else 0
        if dte < MIN_DTE or dte > MAX_DTE:
            continue
            
        c_ask = max(c_b, c_s)
        c_bid = min(c_b, c_s)
        p_ask = max(p_b, p_s)
        p_bid = min(p_b, p_s)
        c_mid = (c_b + c_s) / 2
        p_mid = (p_b + p_s) / 2
        spot = c['spot_price']
        
        strike_data.append({
            'K': K,
            'c_ask': c_ask, 'c_bid': c_bid,
            'p_ask': p_ask, 'p_bid': p_bid,
            'c_mid': c_mid, 'p_mid': p_mid,
            'spot': spot, 'dte': dte
        })
    
    results = {}
    # We'll consider pairs where K1 < K2
    for i, s1 in enumerate(strike_data):
        K1 = s1['K']
        c1_ask, c1_bid = s1['c_ask'], s1['c_bid']
        p1_ask, p1_bid = s1['p_ask'], s1['p_bid']
        c1_mid, p1_mid = s1['c_mid'], s1['p_mid']
        spot, dte = s1['spot'], s1['dte']
        
        for s2 in strike_data[i+1:]:
            K2 = s2['K']
            c2_ask, c2_bid = s2['c_ask'], s2['c_bid']
            p2_ask, p2_bid = s2['p_ask'], s2['p_bid']
            c2_mid, p2_mid = s2['c_mid'], s2['p_mid']
            
            # Conservative pricing: buy at ask, sell at bid
            # For long box: we buy call K1 (ask), sell call K2 (bid), buy put K2 (ask), sell put K1 (bid)
            box_buy = (c1_ask - c2_bid) + (p2_ask - p1_bid)
            
            # For exiting the long box: we sell call K1 (bid), buy call K2 (ask), sell put K2 (bid), buy put K1 (ask)
            box_sell = (c1_bid - c2_ask) + (p2_bid - p1_ask)
            
            # Mid prices for marking to market
            box_mid = (c1_mid - c2_mid) + (p2_mid - p1_mid)
            
            results[(K1, K2)] = {
                'box_buy': box_buy,
                'box_sell': box_sell,
                'box_mid': box_mid,
                'K1': K1,
                'K2': K2,
                'spot': spot,
                'dte': dte
            }
    return results

def main():
    dates = sorted([d for d in os.listdir(DATA_DIR) if d.startswith('202603') and not d.startswith('bad')])
    dates = [d for d in dates if d >= START_DATE]
    
    books = {idx: [] for idx in INDICES}
    positions = {idx: None for idx in INDICES} # {type: 'BOX'|'CASH', K1, K2, cash_balance, last_date, width, initial_capital}
    
    print(f"Box Spread Replicator (1-6 month) - Cross-Expiry & Cross-Strike Optimization")
    print(f"Start: {START_DATE} | Fee: {FEE_PER_CONTRACT} | Cash Yield: {CASH_YIELD}%")
    print("-" * 80)
    
    for date in dates:
        dt_curr = datetime.strptime(date, '%Y%m%d')
        for code, name in INDICES.items():
            # Find all files for this index on this date (multiple expiries)
            pattern = os.path.join(DATA_DIR, date, f"{name}_*_{date}.csv")
            files = glob.glob(pattern)
            if not files: 
                continue
            
            # Global box spread data for this index on this date
            all_box_data = {}
            for f in files:
                df = pd.read_csv(f)
                box_data = get_box_spread_price(df)
                for key, vals in box_data.items():
                    # Select the best box (highest yield) for this strike pair across multiple expiries/files
                    width = vals['K2'] - vals['K1']
                    t = vals['dte'] / 365.0
                    # Use mid price for evaluation yield to be more stable, but buy price for execution
                    y = -np.log(vals['box_buy'] / width) / t * 100 if (t > 0 and vals['box_buy'] > 0) else -np.inf
                    vals['yield'] = y
                    
                    if key not in all_box_data or y > all_box_data[key]['yield']:
                        all_box_data[key] = vals
            
            if not all_box_data:
                continue
            
            def box_score(key):
                vals = all_box_data[key]
                width = vals['K2'] - vals['K1']
                if width <= 0 or vals['box_buy'] <= 0:
                    return -np.inf
                t = vals['dte'] / 365.0
                if t <= 0:
                    return -np.inf
                yield_approx = -np.log(vals['box_buy'] / width) / t * 100
                return yield_approx
            
            best_key = max(all_box_data.keys(), key=box_score)
            best_yield = box_score(best_key)
            best_box = all_box_data[best_key]
            best_K1, best_K2 = best_key
            best_box_buy = best_box['box_buy']
            best_box_mid = best_box['box_mid']
            spot = best_box['spot']
            if positions[code] is None:
                # Initial setup
                initial_width = best_K2 - best_K1
                if best_yield > CASH_YIELD:
                    positions[code] = {
                        'type': 'BOX',
                        'K1': best_K1,
                        'K2': best_K2,
                        'cash_balance': - (best_box_buy + 4 * FEE_PER_CONTRACT),
                        'last_date': date,
                        'width': initial_width,
                        'initial_capital': initial_width
                    }
                    action = 'OPEN'
                else:
                    positions[code] = {
                        'type': 'CASH',
                        'cash_balance': 0,
                        'last_date': date,
                        'width': initial_width,
                        'initial_capital': initial_width
                    }
                    action = 'CASH'
            else:
                # Update P&L based on time elapsed
                dt_prev = datetime.strptime(positions[code]['last_date'], '%Y%m%d')
                days_diff = (dt_curr - dt_prev).days
                
                # Accrue cash yield on the cash portion
                daily_yield = (CASH_YIELD / 100) / 365.0
                # Interest on negative cash is cost, positive is gain
                gain = positions[code]['cash_balance'] * daily_yield * days_diff
                positions[code]['cash_balance'] += gain
                positions[code]['last_date'] = date
                
                if positions[code]['type'] == 'CASH':
                    if best_yield > CASH_YIELD + 0.5: # Small buffer to avoid flickering
                        # Buy BOX
                        positions[code]['type'] = 'BOX'
                        positions[code]['K1'] = best_K1
                        positions[code]['K2'] = best_K2
                        positions[code]['cash_balance'] -= (best_box_buy + 4 * FEE_PER_CONTRACT)
                        positions[code]['width'] = best_K2 - best_K1
                        action = 'BUY BOX'
                    else:
                        action = 'HOLD CASH'
                else:
                    # Current type is BOX
                    curr_K1 = positions[code]['K1']
                    curr_K2 = positions[code]['K2']
                    curr_key = (curr_K1, curr_K2)
                    
                    # Handle Expiry/Settlement
                    if curr_key in all_box_data and all_box_data[curr_key]['dte'] <= 1:
                         # Settlement at maturity: box is worth its width
                         positions[code]['cash_balance'] += (curr_K2 - curr_K1)
                         positions[code]['type'] = 'CASH'
                         action = 'SETTLE'
                    elif curr_key in all_box_data:
                        # Mark-to-Market current yield
                        curr_box_mid = all_box_data[curr_key]['box_mid']
                        curr_box_sell = all_box_data[curr_key]['box_sell']
                        curr_dte = all_box_data[curr_key]['dte']
                        t_rem = curr_dte / 365.0
                        curr_yield = -np.log(curr_box_mid / (curr_K2 - curr_K1)) / t_rem * 100 if t_rem > 0 else -np.inf
                        
                        # Roll friction check:
                        # Transaction cost to roll: spread on exit + spread on entry
                        # We use 4 * FEE_PER_CONTRACT as a placeholder for broker fees
                        cost_to_exit = curr_box_mid - curr_box_sell
                        cost_to_enter = best_box_buy - best_box_mid
                        total_friction = cost_to_exit + cost_to_enter + 8 * FEE_PER_CONTRACT
                        
                        # Extra yield gain in points over the remaining life:
                        yield_diff = best_yield - curr_yield
                        extra_gain_pts = (yield_diff / 100) * t_rem * (curr_K2 - curr_K1)
                        
                        # Extra gain if we move to cash:
                        cash_yield_diff = CASH_YIELD - curr_yield
                        cash_extra_gain_pts = (cash_yield_diff / 100) * t_rem * (curr_K2 - curr_K1)
                        
                        # Decide: Roll, Hold, or Sell to Cash
                        if cash_yield_diff > 0.5 and cash_extra_gain_pts > cost_to_exit * 1.2:
                            # Yield advantage of cash beats the exit cost (plus small buffer)
                            positions[code]['cash_balance'] += (curr_box_sell - 4 * FEE_PER_CONTRACT)
                            positions[code]['type'] = 'CASH'
                            positions[code]['width'] = (curr_K2 - curr_K1)
                            action = 'SELL->CASH'
                        elif yield_diff > 1.0 and extra_gain_pts > total_friction * 1.5: # 1.5x coverage buffer
                            # Roll: Sell current, buy best
                            roll_credit = curr_box_sell - (best_box_buy + 8 * FEE_PER_CONTRACT)
                            positions[code]['cash_balance'] += roll_credit
                            positions[code]['K1'] = best_K1
                            positions[code]['K2'] = best_K2
                            positions[code]['width'] = best_K2 - best_K1
                            action = f"ROLL->K{best_K1}-K{best_K2}"
                        else:
                            action = "HOLD"
                    else:
                        # Current box not found in today's data (illiquid)
                        action = 'HOLD (ILLIQ)'
            
            # Record state for reporting (Mark-to-Market)
            cash = positions[code]['cash_balance']
            if positions[code]['type'] == 'BOX':
                active_K1 = positions[code]['K1']
                active_K2 = positions[code]['K2']
                active_key = (active_K1, active_K2)
                width = active_K2 - active_K1
                
                if active_key in all_box_data:
                    active_box_mid = all_box_data[active_key]['box_mid']
                    active_dte = all_box_data[active_key]['dte']
                    active_yield = all_box_data[active_key]['yield']
                else:
                    active_box_mid = books[code][-1]['BoxMid'] if books[code] else width
                    active_dte = 0
                    active_yield = 0
                
                box_value = active_box_mid
            else:
                active_K1 = active_K2 = 0
                active_box_mid = 0
                active_yield = CASH_YIELD
                box_value = 0
            
            nav = cash + box_value
            pnl = nav # Since initial cash was 0 or handled relative to capital
            initial_cap = positions[code]['initial_capital']
            total_return = (pnl / initial_cap * 100) if initial_cap > 0 else 0
            
            books[code].append({
                'Date': date,
                'Type': positions[code]['type'],
                'K1': active_K1,
                'K2': active_K2,
                'Width': width,
                'BoxMid': round(active_box_mid, 1),
                'AnnYield': round(active_yield, 2),
                'BestYield': round(max(best_yield, CASH_YIELD), 2),
                'NAV': round(nav, 1),
                'PnL%': round(total_return, 2),
                'Action': action
            })
    
    for code in INDICES:
        print(f"\n--- {code} ({INDICES[code]}) ---")
        df_book = pd.DataFrame(books[code])
        if not df_book.empty:
            print(df_book.to_string(index=False))
        else:
            print("No data")

if __name__ == "__main__":
    main()