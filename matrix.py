import pandas as pd
import glob
import os
import sys

def get_option_matrix(date, option_type):
    # Mapping for common prefixes to Chinese names
    mapping = {
        'IO': '沪深300股指期权',
        'HO': '上证50股指期权',
        'MO': '中证1000股指期权'
    }
    
    chinese_name = mapping.get(option_type, option_type)
    data_path = f"option_data/{date}"
    
    if not os.path.isdir(data_path):
        print(f"Directory {data_path} not found.")
        return

    # Find matching files
    pattern = os.path.join(data_path, f"{chinese_name}_*.csv")
    files = glob.glob(pattern)
    
    if not files:
        # Try a more broad search if specific name fails
        pattern = os.path.join(data_path, "*.csv")
        files = [f for f in glob.glob(pattern) if option_type in f or chinese_name in f]
    
    if not files:
        print(f"No files found for {option_type} on {date} in {data_path}")
        return

    all_data = []
    for f in files:
        try:
            df = pd.read_csv(f)
            # Ensure columns are cleaned
            df.columns = df.columns.str.strip()
            all_data.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
    
    if not all_data:
        print("No valid data loaded.")
        return

    df = pd.concat(all_data, ignore_index=True)
    
    # Process cell value: (bprice sprice)
    # Using .fillna(0) for missing prices to ensure string format works
    df['bprice'] = df['bprice'].fillna(0)
    df['sprice'] = df['sprice'].fillna(0)
    df['price_str'] = df.apply(lambda r: f"{r['bprice']},{r['sprice']}", axis=1)
    
    # Time to expire (T) - descending T_1 > T_2
    # Strikes (K) - ascending K_1 < K_2
    
    for t_val in ['C', 'P']:
        label = "Call" if t_val == 'C' else "Put"
        sub = df[df['type'] == t_val]
        if sub.empty:
            print(f"\nNo {label} data found.")
            continue
            
        print(f"\n### {label} Matrix ({option_type} - {date})")
        
        # Pivot by strike and days_to_expire (DTE as Proxy for T)
        pivot = sub.pivot(index='strike', columns='days_to_expire', values='price_str')
        
        # Sort columns descending (T_1 > T_2)
        expiries = sorted(pivot.columns, reverse=True)
        pivot = pivot[expiries]
        
        # Sort index ascending (K_1 < K_2)
        pivot = pivot.sort_index()
        
        # Content formatting with aligned columns
        rows = []
        header = [label] + [f"T={e}" for e in expiries]
        rows.append(header)
        
        for strike, row in pivot.iterrows():
            cells = [str(strike)] + [str(row[e]) if pd.notna(row[e]) else "null,null" for e in expiries]
            rows.append(cells)
            
        # Calculate max width for each column
        col_widths = [max(len(str(row[i])) for row in rows) for i in range(len(header))]
        
        # Print header
        print("| " + " | ".join(rows[0][i].ljust(col_widths[i]) for i in range(len(header))) + " |")
        print("| " + " | ".join("-" * col_widths[i] for i in range(len(header))) + " |")
        
        # Print content rows
        for row in rows[1:]:
            print("| " + " | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(header))) + " |")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        # Default for convenience if no args provided
        date = "20260320"
        otype = "IO"
        print(f"Usage: python matrix.py <date> <type> (using {date} {otype} as default)")
    else:
        date = sys.argv[1]
        otype = sys.argv[2]
        
    get_option_matrix(date, otype)
