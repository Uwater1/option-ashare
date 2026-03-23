import os
import glob
import pandas as pd
import argparse
import numpy as np
import itertools

def main():
    parser = argparse.ArgumentParser(description="Detect TP2 Violations in Options")
    parser.add_argument('data_dir', type=str, help='Directory containing option CSV files')
    args = parser.parse_args()
    
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
    
    required = {'ticker', 'type', 'strike', 'days_to_expire', 'bprice', 'sprice', 'future_price'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}")
        
    def get_underlying(ticker: str) -> str:
        if not isinstance(ticker, str): return "Unknown"
        for prefix in ['HO', 'IO', 'MO']:
            if ticker.startswith(prefix):
                return prefix
        return ticker[:2] if len(ticker) >= 2 else ticker
        
    df['underlying'] = df['ticker'].apply(get_underlying)
    
    # 1. Filter to OTM options only:
    # Call: K > S
    # Put: K < S_0
    df['is_otm'] = False
    df.loc[(df['type'] == 'C') & (df['strike'] > df['future_price']), 'is_otm'] = True
    df.loc[(df['type'] == 'P') & (df['strike'] < df['future_price']), 'is_otm'] = True
    
    # Also require positive prices and positive DTE
    df = df[df['is_otm'] & (df['days_to_expire'] > 0) & (df['sprice'] > 0) & (df['bprice'] > 0)]
    
    results = []
    
    # 2. For each option type separately
    groups = df.groupby(['underlying', 'type'])
    for (und, otype), group in groups:
        # Pivot the data to matrices: rows = strikes, cols = days_to_expire
        grouped_k_t = group.groupby(['strike', 'days_to_expire']).first().reset_index()
        
        strikes = np.sort(grouped_k_t['strike'].unique())
        dtes = np.sort(grouped_k_t['days_to_expire'].unique())
        
        if len(strikes) < 2 or len(dtes) < 2:
            continue
            
        k_to_idx = {k: i for i, k in enumerate(strikes)}
        t_to_idx = {t: i for i, t in enumerate(dtes)}
        
        bprice_mat = np.full((len(strikes), len(dtes)), np.nan)
        sprice_mat = np.full((len(strikes), len(dtes)), np.nan)
        
        for _, row in grouped_k_t.iterrows():
            i = k_to_idx[row['strike']]
            j = t_to_idx[row['days_to_expire']]
            bprice_mat[i, j] = row['bprice']  # bid / sell price
            sprice_mat[i, j] = row['sprice']  # ask / buy price
            
        # 6. Optimize: Vectorization over all combinations of 2 strikes and 2 dtes
        k_pairs = list(itertools.combinations(range(len(strikes)), 2))
        t_pairs = list(itertools.combinations(range(len(dtes)), 2))
        
        if not k_pairs or not t_pairs:
            continue
            
        k1_idx = np.array([p[0] for p in k_pairs])
        k2_idx = np.array([p[1] for p in k_pairs])
        
        t1_idx = np.array([p[0] for p in t_pairs])
        t2_idx = np.array([p[1] for p in t_pairs])
        
        K1_mesh, T1_mesh = np.meshgrid(k1_idx, t1_idx, indexing='ij')
        K2_mesh, T2_mesh = np.meshgrid(k2_idx, t2_idx, indexing='ij')
        
        k1_flat = K1_mesh.flatten()
        k2_flat = K2_mesh.flatten()
        t1_flat = T1_mesh.flatten()
        t2_flat = T2_mesh.flatten()
        
        # 4. sprice/bprice handling
        # TP2 theoretical inequality: P(K1,T1) * P(K2,T2) >= P(K1,T2) * P(K2,T1)
        # To find executable violations, we "buy" LHS and "sell" RHS
        # LHS: ask_11 * ask_22
        # RHS: bid_12 * bid_21
        ask_11 = sprice_mat[k1_flat, t1_flat]
        ask_22 = sprice_mat[k2_flat, t2_flat]
        
        bid_12 = bprice_mat[k1_flat, t2_flat]
        bid_21 = bprice_mat[k2_flat, t1_flat]
        
        # We may also consider the case where the user meant '+' instead of '*'. 
        # But based on the TP2 context strictly, it is a product.
        cost = ask_11 * ask_22
        revenue = bid_12 * bid_21
        
        # 3. Violation metric: Delta = RHS - LHS
        delta = revenue - cost
        
        # 4. Only flag with positive net edge
        # We need all prices to be valid (not NaN)
        valid = (delta > 0) & ~np.isnan(delta) & (cost > 0)
        
        for idx in np.where(valid)[0]:
            k1 = strikes[k1_flat[idx]]
            k2 = strikes[k2_flat[idx]]
            t1 = dtes[t1_flat[idx]]
            t2 = dtes[t2_flat[idx]]
            
            val_delta = delta[idx]
            val_cost = cost[idx]
            norm_score = val_delta / val_cost if val_cost > 0 else 0
            
            results.append({
                'Underlying': und,
                'Type': otype,
                'K1': k1, 'K2': k2,
                'T1': int(t1), 'T2': int(t2),
                'Delta': val_delta,
                'Normalized Score': norm_score,
                'Details': f"Buy (K={k1},T={int(t1)})@{ask_11[idx]:.4f} & (K={k2},T={int(t2)})@{ask_22[idx]:.4f} | Sell (K={k1},T={int(t2)})@{bid_12[idx]:.4f} & (K={k2},T={int(t1)})@{bid_21[idx]:.4f}"
            })
            
    # 5. Output
    if not results:
        print("No TP2 violations (with positive net edge) found.")
        return
        
    res_df = pd.DataFrame(results)
    
    # 5. Rank by Delta
    res_df.sort_values('Delta', ascending=False, inplace=True)
    
    print(f"Found {len(res_df)} TP2 violations.")
    
    print("\nTop Violations by Delta:")
    
    # Header for the custom form output
    header_fmt = f"{'Underlying':<12} {'Type':<6} {'K1':<10} {'K2':<10} {'T1':<6} {'T2':<6} {'Delta':<15} {'Normalized Score':<18}"
    print("\n" + header_fmt)
    print("-" * 120)
    
    for _, row in res_df.iterrows():
        # First row: Underlying Type K1 K2 T1 T2 Delta Normalized Score
        line1 = f"{str(row['Underlying']):<12} {str(row['Type']):<6} {row['K1']:<10} {row['K2']:<10} {int(row['T1']):<6} {int(row['T2']):<6} {row['Delta']:<15.6f} {row['Normalized Score']:<18.6f}"
        print(line1)
        # Second row: Details
        print(f"{row['Details']}")
        # Separator
        print("-" * 120)

if __name__ == '__main__':
    main()
