import akshare as ak

target_date = "20260213"
futures_comm_js_df = ak.futures_comm_info(symbol="中国金融期货交易所")
print(futures_comm_js_df)

if not futures_comm_js_df.empty:
    filename = f"futures_comm_{target_date}.csv"
    futures_comm_js_df.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"Data saved to {filename}")
else:
    print("No data found for the given date.")