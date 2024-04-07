import pandas as pd
import yfinance as yf
from datetime import datetime,timedelta
from concurrent.futures import ThreadPoolExecutor
def fetch_stock_data(ticker, start_date, end, interval):
    stock_data = yf.Ticker(ticker)
    stock_data = stock_data.history(start=start_date, end=end, interval=interval)
    stock_data["Volume"] = stock_data["Volume"] * ((stock_data["Close"] + stock_data['Open']) / 2)
    return stock_data


def active_stocks(merged_df, ticker_list, date):
    N = 0
    for ticker in ticker_list:
        if pd.isna(merged_df[ticker]['Close'][date]) is False:
            N+=1
    return N
def weight_add(ticker_list,start_date,base_price_close = 1000, end=None, interval = '1d', MA=None):
    
    ## create stock price dataset
    df_list = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_stock_data, ticker, start_date, end, interval) for ticker in ticker_list]
        for future in futures:
            df_list.append(future.result())

    merged_df = pd.concat(df_list, axis=1, keys=ticker_list)
    return merged_df


    merged_df = pd.concat(df_list, axis=1, keys=ticker_list)

    
    ## create stock price dataset

    
    merged_df.index = merged_df.index.date
    st_date = merged_df.index[0]
    for ticker in ticker_list:
        if 'Weight' not in merged_df[ticker]:
            merged_df[ticker, 'Weight'] = None

    ## initial parameters
    
    print(st_date)
    en_date = st_date + timedelta(days=180)
    total_val = base_price_close
    N = active_stocks(merged_df, ticker_list, st_date)
    dollar_amount = total_val/N
    
    ## weight calculations
    for date in merged_df.index:
        # assign normal weight
        for ticker in ticker_list:
            merged_df.loc[date, (ticker, 'Weight')] = dollar_amount/merged_df.loc[st_date, (ticker, 'Close')]
        # rebalencing    
        if date>en_date:
            st_date = date
            en_date = st_date + timedelta(days=180)
            val = 0
            for ticker in ticker_list:
                if pd.isna(merged_df[ticker, 'Weight'][date]) is False:
                    val += merged_df[ticker,'Weight'][date] * merged_df[ticker]["Close"][date]
            total_val = val
            N = active_stocks(merged_df, ticker_list, date)
            dollar_amount = total_val/N

    ## calculate index
    for price_type in ['Open', "High", 'Low', 'Close']:
        df = 0
        for ticker in ticker_list:
            k = (merged_df[ticker, price_type]*merged_df[ticker, 'Weight'])
            k.fillna(0, inplace=True)
            df += k
        merged_df['Weighted', price_type] = df
    ## volumn calculations
    df = 0
    for ticker in ticker_list:
        k = merged_df[ticker, 'Volume']*merged_df[ticker, 'Close']
        k.fillna(0, inplace=True)
        df += k
    merged_df['Weighted', 'Volume'] = df
   # print(merged_df["Weighted"])
    return merged_df["Weighted"]

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

    stock_l = stock_list(file_path)
 

    merged_df = weight_add(stock_l, start_date=start_date,interval= interval,base_price_close = 1000)
    merged_df.reset_index(drop=False, inplace=True)
    merged_df.rename(columns={'index': 'Date'}, inplace=True)
    merged_df = merged_df.round(2)
    merged_df['Date'] = pd.to_datetime(merged_df['Date']).dt.strftime('%Y-%m-%d') 
    
    if MA is not None:
        for num in MA:
            merged_df['MA'+str(num)] = merged_df.Close.rolling(num).mean()
    
    merged_df.fillna(value=0, inplace=True)
    
    
    return merged_df


