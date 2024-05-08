import pandas as pd
import yfinance as yf
import time
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


def weight_add(ticker_list,start_date,base_price_close = 1000, end="2024-04-01", interval = '1d', MA=None):
    a= time.time()
    df_list = []
    print(ticker_list)
    print(len(ticker_list))
    for ticker in ticker_list:
        stock_data = yf.Ticker(ticker)
        stock_data = stock_data.history(start = start_date, end=end, interval=interval)
        if stock_data.empty:
            ticker_list.remove(ticker)
            pass
        stock_data["Volume"] = stock_data["Volume"]*((stock_data["Close"]+stock_data['Open'])/2)
        df_list.append(stock_data)
    


    merged_df = pd.concat(df_list, axis=1, keys=ticker_list)
    
    b = time.time()
    print(b-a)

    
    

    
   # merged_df.index = merged_df.index.date
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
    merged_df.to_csv(r"C:\Users\yaswa\Downloads\ORBFinal.csv")
   # print(merged_df["Weighted"])
    return merged_df["Weighted"]

def get_stock_data(file_path, start_date="2015-01-01", end= "2024-04-01", interval='1d', MA=[30]):
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

    ticker_list = stock_list(file_path)
    stock_l = stock_selection(ticker_list,start_date=start_date)

 

    merged_df = weight_add(stock_l, start_date=start_date,interval= interval,base_price_close = 1000)
    merged_df.reset_index(drop=False, inplace=True)
    merged_df.rename(columns={'index': 'Date'}, inplace=True)
    merged_df = merged_df.round(2)
    merged_df['Date'] = pd.to_datetime(merged_df['Date']).dt.strftime('%Y-%m-%d') 
    
    if MA is not None:
        for num in MA:
            merged_df['MA'+str(num)] = merged_df.Close.rolling(num).mean()
    
    merged_df.fillna(value=0, inplace=True)
    #merged_df.to_csv(r"C:\Users\yaswa\Downloads\ORB_Final.csv",index  = False)
    
    
    return merged_df
def stock_selection(ticker_list,start_date,end ='2024-04-01'):
    global f
    #By 3 month and 6 month gain.
    f= {}
    for ticker in ticker_list:
        stock_data = yf.Ticker(ticker)
        data = stock_data.history(start = start_date, end=end, interval='1d')
        if data.empty:
            ticker_list.remove(ticker)
            pass
        if len(data) < 65:
            pass
        if 65<=len(data) < 130:
            r2 = data.tail(65)
            low_3m = min(r2["Close"])
            close_change_3m = ((r2["Close"][-1] / low_3m) - 1) * 100
            f[ticker] = close_change_3m
        if len(data) >=130:
            r1 = data.tail(130)
            low_6m = min(r1["Close"])
            r2 = data.tail(65)
            low_3m = min(r2["Close"])
            close_change_6m = ((r1["Close"][-1] /low_6m) - 1) * 100
            close_change_3m = ((r2["Close"][-1] / low_3m) - 1) * 100
            f[ticker] = max(close_change_6m,close_change_3m)


    sorted_tickers = sorted(f, key=f.get, reverse=True)
    if len(sorted_tickers)<15:
        return sorted_tickers
    first_five = sorted_tickers[:15]
    middle_index = len(sorted_tickers) // 2
    middle_three = sorted_tickers[middle_index - 1 : middle_index + 2]
    #first_five.extend(middle_three)
    return first_five
    
