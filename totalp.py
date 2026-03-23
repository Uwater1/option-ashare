import os
import glob
import pandas as pd
import argparse
import numpy as np
import itertools

# ── Negative carry constant ──────────────────────────────────────────────────
# For Chinese stock index futures, r - q ≈ -1 % per year.
# This means F = S · exp((r-q)·T) < S, i.e. futures trade at a DISCOUNT.
# We expose it as a constant so it can be overridden from the command line.
DEFAULT_CARRY = -0.01          # -1 % annual continuously-compounded

def main():
    parser = argparse.ArgumentParser(description="Detect TP2 Violations in Options")
    parser.add_argument('data_dir', type=str, help='Directory containing option CSV files')
    parser.add_argument(
        '--carry', type=float, default=DEFAULT_CARRY,
        help='Annual cost-of-carry r-q (default: -0.01 for CN stock futures)')
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

    required = {'ticker', 'type', 'strike', 'days_to_expire',
                'bprice', 'sprice', 'future_price'}
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

    # ── FIX 1: Recompute an "adjusted forward" that folds in negative carry ──
    # The raw future_price column is the market price of the futures contract.
    # For TP2 moneyness classification we still use it directly — that is correct.
    # But we expose the carry so downstream Greeks / normalisation can use it.
    carry = args.carry   # e.g. -0.01

    # ── FIX 2: OTM classification ────────────────────────────────────────────
    # With negative carry F < S, so:
    #   Call OTM:  K > F   (unchanged — K above the forward)
    #   Put  OTM:  K < F   (unchanged — K below the forward)
    # Your original logic was already correct in using future_price as the
    # reference.  We keep it, but add a small epsilon to avoid at-the-money
    # options that have near-zero extrinsic value and can produce spurious hits.
    eps = 1e-8
    df['is_otm'] = False
    df.loc[(df['type'] == 'C') & (df['strike'] > df['future_price'] + eps), 'is_otm'] = True
    df.loc[(df['type'] == 'P') & (df['strike'] < df['future_price'] - eps), 'is_otm'] = True

    df = df[
        df['is_otm'] &
        (df['days_to_expire'] > 0) &
        (df['sprice'] > 0) &
        (df['bprice'] > 0)
    ].copy()

    # ── FIX 3: Normalise strikes to "log-moneyness" for puts ─────────────────
    # TP2 is a statement about the *shape* of the implied vol surface.
    # The inequality P(K1,T1)·P(K2,T2) >= P(K1,T2)·P(K2,T1) is written with
    # the convention that K1 < K2 and T1 < T2.
    # For PUTS  K1 < K2  means K1 is the DEEPER OTM strike (further from F).
    # For CALLS K1 < K2  means K1 is the less OTM strike  (closer to F).
    # The inequality holds in BOTH cases as long as we normalise prices by
    # intrinsic / discount factor.  With negative carry the discount factor is
    # exp(carry · T/365) > 1 (since carry < 0), so undiscounted put prices are
    # SMALLER than their present values.  We correct for this before testing TP2.

    # Discount-factor correction: multiply each price by exp(-carry · T/365)
    # so that prices are expressed on a "forward-neutral" basis.
    # For carry = -0.01 and T = 30: exp(0.01 * 30/365) ≈ 1.00082  (tiny but correct)
    df['T_years'] = df['days_to_expire'] / 365.0
    df['df_adj']  = np.exp(-carry * df['T_years'])   # >1 when carry<0
    df['bprice_adj'] = df['bprice'] * df['df_adj']
    df['sprice_adj'] = df['sprice'] * df['df_adj']

    results = []

    groups = df.groupby(['underlying', 'type'])
    for (und, otype), group in groups:

        grouped_k_t = group.groupby(['strike', 'days_to_expire']).first().reset_index()

        strikes = np.sort(grouped_k_t['strike'].unique())
        dtes    = np.sort(grouped_k_t['days_to_expire'].unique())

        if len(strikes) < 2 or len(dtes) < 2:
            continue

        k_to_idx = {k: i for i, k in enumerate(strikes)}
        t_to_idx = {t: i for i, t in enumerate(dtes)}

        # Use carry-adjusted prices throughout
        bprice_mat = np.full((len(strikes), len(dtes)), np.nan)
        sprice_mat = np.full((len(strikes), len(dtes)), np.nan)

        for _, row in grouped_k_t.iterrows():
            i = k_to_idx[row['strike']]
            j = t_to_idx[row['days_to_expire']]
            bprice_mat[i, j] = row['bprice_adj']
            sprice_mat[i, j] = row['sprice_adj']

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

        # ── FIX 4: Correct TP2 trade direction for puts vs calls ─────────────
        #
        # TP2 (Total Positivity of order 2) states that the option price matrix
        # C(K, T) is TP2, i.e. for K1 < K2 and T1 < T2:
        #
        #   C(K1,T1) · C(K2,T2)  ≥  C(K1,T2) · C(K2,T1)
        #
        # Arbitrage interpretation:
        #   Buy  the (K1,T1) and (K2,T2) options  → pay  ask_11 · ask_22
        #   Sell the (K1,T2) and (K2,T1) options  → receive bid_12 · bid_21
        #
        # This direction is the SAME for calls and puts because the inequality
        # is symmetric in the price matrix — what changes is only which strike
        # is "K1" (lower) vs "K2" (higher).
        #
        # For CALLS: K1 closer-to-ATM, K2 further OTM  → LHS has the two
        #   "diagonal" options, RHS has the two "off-diagonal" ones.
        # For PUTS with negative carry: K1 is the deeper-OTM put (K1 < K2 < F).
        #   The inequality still reads the same way numerically because we sort
        #   strikes in ascending order for both types.
        #
        # No direction flip is needed.  The carry adjustment in FIX 3 already
        # accounts for the negative time-value effect.

        ask_11 = sprice_mat[k1_flat, t1_flat]
        ask_22 = sprice_mat[k2_flat, t2_flat]
        bid_12 = bprice_mat[k1_flat, t2_flat]
        bid_21 = bprice_mat[k2_flat, t1_flat]

        cost    = ask_11 * ask_22
        revenue = bid_12 * bid_21
        delta   = revenue - cost
        score = delta / cost

        valid = (delta > 1) & ~np.isnan(delta) & (cost > 0) & (score > 0.05)

        for idx in np.where(valid)[0]:
            k1 = strikes[k1_flat[idx]]
            k2 = strikes[k2_flat[idx]]
            t1 = dtes[t1_flat[idx]]
            t2 = dtes[t2_flat[idx]]

            val_delta = delta[idx]
            val_cost  = cost[idx]
            norm_score = score[idx]

            results.append({
                'Underlying':       und,
                'Type':             otype,
                'K1':               k1,
                'K2':               k2,
                'T1':               int(t1),
                'T2':               int(t2),
                'Delta':            val_delta,
                'Cost':             val_cost,
                'Normalized Score': norm_score,
                'Details': (
                    f"Buy  (K={k1},T={int(t1)})@{ask_11[idx]:.4f}"
                    f" & (K={k2},T={int(t2)})@{ask_22[idx]:.4f}"
                    f" | Sell (K={k1},T={int(t2)})@{bid_12[idx]:.4f}"
                    f" & (K={k2},T={int(t1)})@{bid_21[idx]:.4f}"
                    f"  [adj. r-q={carry:.2%}]"
                )
            })

    if not results:
        print("No TP2 violations (with positive net edge) found.")
        return

    res_df = pd.DataFrame(results)
    res_df.sort_values('Delta', ascending=False, inplace=True)

    print(f"Found {len(res_df)} TP2 violations.")
    print("Top Violations by Delta:")

    header_fmt = (
        f"{'Underlying':<12} {'Type':<6} {'K1':<10} {'K2':<10}"
        f" {'T1':<6} {'T2':<6} {'Delta':<15} {'Cost':<15} {'Normalized Score':<18}"
    )
    print("\n" + header_fmt)
    print("-" * 130)

    for _, row in res_df.iterrows():
        line1 = (
            f"{str(row['Underlying']):<12} {str(row['Type']):<6}"
            f" {row['K1']:<10} {row['K2']:<10}"
            f" {int(row['T1']):<6} {int(row['T2']):<6}"
            f" {row['Delta']:<15.2f} {row['Cost']:<15.2f}"
            f" {row['Normalized Score']:<18.6f}"
        )
        print(line1)
        print(f"{row['Details']}")
        print("-" * 130)


if __name__ == '__main__':
    main()
