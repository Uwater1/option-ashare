import pandas as pd
import numpy as np
import lightgbm as lgb
import os
import glob
import re
import json
import math
import optuna
from numba import njit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# Suppress optuna verbosity
optuna.logging.set_verbosity(optuna.logging.WARNING)

# Constants
R = 0.02  # Risk-free rate

@njit(cache=True)
def cdf_jit(x):
    """Fast normal CDF approximation using math.erf"""
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
        return 0.0
    low = 1e-4
    high = 10.0
    p_low = black_scholes_price_jit(S, K, T, r, low, is_call)
    p_high = black_scholes_price_jit(S, K, T, r, high, is_call)
    if (market_price - p_low) * (market_price - p_high) > 0:
        return 0.0
    for _ in range(50):
        mid = (low + high) / 2.0
        p_mid = black_scholes_price_jit(S, K, T, r, mid, is_call)
        if (market_price - p_mid) > 0:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0

@njit(cache=True)
def calculate_ivs_vec(market_prices, Ss, Ks, Ts, rs, is_calls):
    n = len(market_prices)
    res = np.empty(n)
    for i in range(n):
        res[i] = find_iv_jit(market_prices[i], Ss[i], Ks[i], Ts[i], rs[i], is_calls[i])
    return res

def huber_loss_calc(y_true, y_pred, delta=1.0):
    error = y_true - y_pred
    is_small_error = np.abs(error) <= delta
    squared_loss = 0.5 * np.square(error)
    linear_loss = delta * (np.abs(error) - 0.5 * delta)
    return np.where(is_small_error, squared_loss, linear_loss).mean()

def extract_ticker_prefix(ticker):
    match = re.match(r'([A-Za-z]+)', str(ticker))
    if match:
        return match.group(1)
    return 'UNK'

def load_data(base_dir):
    all_files = glob.glob(os.path.join(base_dir, '202*/*.csv'))
    dfs = []
    for f in all_files:
        try:
            df = pd.read_csv(f)
            required = {'type', 'strike', 'bprice', 'sprice', 'days_to_expire', 'ticker'}
            if not required.issubset(df.columns):
                continue
            # Extract date from directory name
            date_str = os.path.basename(os.path.dirname(f))
            try:
                df['_date'] = pd.to_datetime(date_str, format='%Y%m%d')
            except Exception:
                df['_date'] = pd.NaT
            dfs.append(df)
        except Exception:
            pass
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

def preprocess(df):
    num_cols = ['strike', 'bprice', 'sprice', 'days_to_expire',
                'future_price', 'spot_price']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['bprice', 'sprice', 'strike', 'days_to_expire']).copy()
    df['spread'] = df['sprice'] - df['bprice']
    df = df[df['spread'] >= 0].copy()

    # ── Clip spread outliers at 99th percentile ──────────────────────────────
    spread_cap = df['spread'].quantile(0.99)
    df = df[df['spread'] <= spread_cap].copy()

    df['midprice'] = (df['bprice'] + df['sprice']) / 2
    df['ticker_prefix'] = df['ticker'].apply(extract_ticker_prefix)
    df['S'] = df['future_price'].fillna(df['spot_price'])
    df = df.dropna(subset=['S']).copy()

    T = df['days_to_expire'].values / 365.0
    is_call = (df['type'] == 'C').values

    print("Calculating IV with Numba...")
    df['iv'] = calculate_ivs_vec(
        df['midprice'].values,
        df['S'].values,
        df['strike'].values,
        T,
        np.full(len(df), R),
        is_call
    )

    iv_median = df.loc[df['iv'] > 0, 'iv'].median()
    df.loc[df['iv'] <= 0, 'iv'] = iv_median

    # ── Core option features ─────────────────────────────────────────────────
    df['log_moneyness'] = np.log(df['S'] / df['strike'])
    df['sqrt_dte'] = np.sqrt(T)
    df['log_moneyness_x_sqrt_dte'] = df['log_moneyness'] * df['sqrt_dte']
    df['inv_dte'] = 1.0 / (T + 1e-6)

    # ── NEW: spread_pct (normalised spread) ──────────────────────────────────
    df['spread_pct'] = df['spread'] / (df['midprice'] + 1e-6)

    # ── NEW: OTM depth ───────────────────────────────────────────────────────
    df['otm_depth'] = np.abs(df['log_moneyness'])

    # ── NEW: vega proxy ──────────────────────────────────────────────────────
    df['iv_x_sqrt_dte'] = df['iv'] * df['sqrt_dte']

    # ── NEW: ATM flag ────────────────────────────────────────────────────────
    df['is_atm'] = (df['otm_depth'] < 0.02).astype(np.int8)

    # ── NEW: DTE bucket (categorical) ────────────────────────────────────────
    df['dte_bucket'] = pd.cut(
        df['days_to_expire'],
        bins=[-1, 7, 30, 90, np.inf],
        labels=['near', 'short', 'medium', 'far']
    ).astype('category')


    # ── Categorical encodings ────────────────────────────────────────────────
    df['ticker_cat'] = df['ticker_prefix'].astype('category')
    df['type_cat'] = df['type'].astype('category')

    # ── TARGET: spread percentage (log-transformed) ──────────────────────────
    # We use log(spread_pct) because spreads vary over orders of magnitude.
    df['target'] = np.log(df['spread_pct'].clip(lower=1e-9))

    return df

def objective(trial, X_train, y_train, X_val, y_val):
    params = {
        'objective': 'huber',
        'alpha': trial.suggest_float('alpha', 0.7, 0.99),
        'metric': 'huber',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'lambda_l1': trial.suggest_float('lambda_l1', 1e-8, 10.0, log=True),
        'lambda_l2': trial.suggest_float('lambda_l2', 1e-8, 10.0, log=True),
        'num_leaves': trial.suggest_int('num_leaves', 16, 256),
        'max_depth': trial.suggest_int('max_depth', 3, 12),
        'min_gain_to_split': trial.suggest_float('min_gain_to_split', 0.0, 1.0),
        'feature_fraction': trial.suggest_float('feature_fraction', 0.4, 1.0),
        'bagging_fraction': trial.suggest_float('bagging_fraction', 0.4, 1.0),
        'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
        'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
        'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.3, log=True),
        'n_estimators': 2000,
        'early_stopping_round': 100,
    }

    model = lgb.LGBMRegressor(**params)
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)]
    )

    preds = model.predict(X_val)
    rmse = np.sqrt(mean_squared_error(y_val, preds))
    return rmse

def train():
    df = load_data('option_data')
    if df.empty:
        print("No data found.")
        return
    df = preprocess(df)
    print(f"Total samples after preprocessing: {len(df):,}")

    features = [
        'midprice', 'strike', 'days_to_expire', 'S',
        'log_moneyness', 'iv', 'log_moneyness_x_sqrt_dte',
        'sqrt_dte', 'inv_dte',
        'otm_depth', 'iv_x_sqrt_dte', 'is_atm',
        'ticker_cat', 'type_cat', 'dte_bucket',
    ]

    X = df[features]
    y = df['target']             # ← predict log(spread_pct)
    dates = df['_date']

    # ── Date-based train/test split (no temporal leakage) ───────────────────
    unique_dates = dates.dropna().unique()
    unique_dates = np.sort(unique_dates)
    split_idx = int(len(unique_dates) * 0.8)
    train_dates = set(unique_dates[:split_idx])
    test_dates  = set(unique_dates[split_idx:])

    train_mask = dates.isin(train_dates)
    test_mask  = dates.isin(test_dates)

    X_train_full, y_train_full = X[train_mask], y[train_mask]
    X_test, y_test = X[test_mask], y[test_mask]

    # Further split training into train/val for Optuna
    val_cut = int(len(X_train_full) * 0.8)
    X_train_sub = X_train_full.iloc[:val_cut]
    y_train_sub = y_train_full.iloc[:val_cut]
    X_val       = X_train_full.iloc[val_cut:]
    y_val       = y_train_full.iloc[val_cut:]

    print(f"Train: {len(X_train_full):,}  Val: {len(X_val):,}  Test: {len(X_test):,}")

    # ── Optuna fine-tuning ───────────────────────────────────────────────────
    print("Fine-tuning with Optuna (100 trials)...")
    sampler = optuna.samplers.TPESampler(seed=42)
    pruner  = optuna.pruners.MedianPruner(n_startup_trials=10, n_warmup_steps=5)
    study = optuna.create_study(direction='minimize', sampler=sampler, pruner=pruner)
    study.optimize(
        lambda trial: objective(trial, X_train_sub, y_train_sub, X_val, y_val),
        n_trials=100,
        show_progress_bar=True,
    )
    print("Best parameters:", study.best_params)

    # ── Train final model on full training set ───────────────────────────────
    best_params = study.best_params.copy()
    best_params['objective'] = 'huber'
    best_params['metric']    = 'huber'
    best_params['verbosity'] = -1
    best_params['n_estimators'] = 2000

    model = lgb.LGBMRegressor(**best_params)
    model.fit(
        X_train_full, y_train_full,
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)]
    )

    # ── Evaluation ───────────────────────────────────────────────────────────
    log_pred = model.predict(X_test)
    pred_pct = np.nan_to_num(np.exp(log_pred), nan=0.0)
    true_pct = df.loc[test_mask, 'spread_pct'].values

    # Absolute units (for backward compatibility / intuition)
    test_mids = X_test['midprice'].values
    y_pred_abs = pred_pct * test_mids
    y_true_abs = df.loc[test_mask, 'spread'].values

    # Primary Metrics (Percentage)
    rmse_pct = np.sqrt(mean_squared_error(true_pct, pred_pct))
    mae_pct  = mean_absolute_error(true_pct, pred_pct)
    r2       = r2_score(true_pct, pred_pct)
    
    # Secondary Metrics (Absolute Value)
    rmse_abs = np.sqrt(mean_squared_error(y_true_abs, y_pred_abs))
    mae_abs  = mean_absolute_error(y_true_abs, y_pred_abs)

    # Residual percentile table (using absolute spread for intuition)
    residuals_abs = np.abs(y_true_abs - y_pred_abs)
    pct_labels = [25, 50, 75, 90, 99]
    pct_values = np.percentile(residuals_abs, pct_labels)

    print("\n" + "=" * 38)
    print("       MODEL EVALUATION RESULTS       ")
    print("-" * 38)
    print(f" Model : LightGBM (log %spread prediction)")
    print(f" Split : Date-based (80/20)")
    print(f" RMSE (%) : {rmse_pct:.6f}")
    print(f" MAE  (%) : {mae_pct:.6f}")
    print(f" R²       : {r2:.6f}")
    print(f"")
    print(f" RMSE (Val): {rmse_abs:.6f}")
    print(f" MAE  (Val): {mae_abs:.6f}")
    print("-" * 38)
    print("  |Residual Value| Percentiles")
    for p, v in zip(pct_labels, pct_values):
        print(f"   P{p:02d} : {v:.4f}")
    print("=" * 38 + "\n")

    # ── Feature importance ───────────────────────────────────────────────────
    importance = pd.Series(
        model.feature_importances_,
        index=features
    ).sort_values(ascending=False)
    print("Top-10 Feature Importances:")
    print(importance.head(10).to_string())
    print()

    # ── Save model & metadata ────────────────────────────────────────────────
    model.booster_.save_model('spread_model.txt')

    categories = {
        'tickers': df['ticker_prefix'].astype('category').cat.categories.tolist(),
        'types':   df['type'].astype('category').cat.categories.tolist(),
        'dte_buckets': ['near', 'short', 'medium', 'far'],
    }
    with open('model_categories.json', 'w') as f:
        json.dump(categories, f)

    print("Training complete. Model saved to spread_model.txt")

if __name__ == '__main__':
    train()
