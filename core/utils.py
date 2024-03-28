import pandas as pd
import yfinance as yf
from datetime import datetime,time

def get_stock_data(file_path, start_date="2015-01-01", end=None, interval='1d', MA=[30]):
    def stock_list(file_path):
        components = []
        with open(file_path, 'r') as file:
            for line in file:
                # Remove leading and trailing whitespace
                line = line.strip()
                if line.startswith("NSE:") or line.startswith("BSE:"):
                    # Format 1: Components separated by commas
                    if "," in line:
                        for component in line.split(","):
                            if component.startswith("NSE:"):
                                components.append(component.replace("NSE:", "") + ".NS")
                            elif component.startswith("BSE:"):
                                components.append(component.replace("BSE:", "") + ".BO")
                            else:
                                components.append(line)  # Append line as is
                    # Format 2: Components separated by whitespace
                    else:
                        if line.startswith("NSE:"):
                            components.append(line.replace("NSE:", "") + ".NS")
                        elif line.startswith("BSE:"):
                            components.append(line.replace("BSE:", "") + ".BO")
                        else:
                            components.append(line)  # Append line as is
                else:
                    components.append(line)  # Append line as is
        return list(set(components))
    ticker_list = []
    ticker_list = stock_list(file_path)
    current_time = datetime.now().time()
    four_pm = time(16, 0)
    if current_time > four_pm:
        end = None
    else:
        end = datetime.now().strftime('%Y-%m-%d')      
    df_list = []
    merged_df = pd.DataFrame()
    
    for ticker in ticker_list:
        stock_data = yf.Ticker(ticker)
        stock_data = stock_data.history(start=start_date, end=end, interval=interval)
        stock_data["Volume"] = stock_data["Volume"] * ((stock_data["Close"] + stock_data['Open']) / 2)
        df_list.append(stock_data)
    
    merged_df = pd.concat(df_list, axis=1, keys=ticker_list)
    
    for price_type in ['Open', 'High', 'Low', 'Close', 'Volume']:
        merged_df['Average', price_type] = merged_df[[(key, price_type) for key in ticker_list]].mean(axis=1, skipna=True)
        

    merged_df = merged_df['Average']
    merged_df.reset_index(drop=False, inplace=True)
    merged_df.rename(columns={'index': 'Date'}, inplace=True)
    merged_df = merged_df.round(2)
    merged_df['Date'] = pd.to_datetime(merged_df['Date']).dt.strftime('%Y-%m-%d') 
    
    if MA is not None:
        for num in MA:
            merged_df['MA'+str(num)] = merged_df.Close.rolling(num).mean()
    
    merged_df.fillna(value=0, inplace=True)
    merged_df.to_csv(r"C:\Users\yaswa\Downloads\merged.csv")
    
    return merged_df
