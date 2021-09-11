import pandas as pd
import datetime as dt
import time
from smartapi import SmartConnect
import requests
import glob
import os.path


def uds_raw_data():

    # files
    inputfile = r'C:\Users\sukhw\OneDrive\Documents\TradingStrategy\NSE500.csv'
    outputfiles = r'C:\Users\sukhw\OneDrive\Documents\TradingStrategy\files'
    
    # date and time
    dateStart = (dt.datetime.today() - dt.timedelta(days=150)).strftime("%Y-%m-%d %H:%M")
    dateEnd = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%Y-%m-%d %H:%M")
    today = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%Y-%m-%d")
    
    
    todayweek = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%V")
    todaymonth = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%m")
    currentdate = str(dt.datetime.today() - dt.timedelta(days=14))
    
    
    # -----------------------------------Get the list of tickers
    # Get Nifty 50 Tickers
    niftydf = pd.read_csv (inputfile)  
    
    # -----------------------------------Get the list of Options tickers as per expiry date
    urlk = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    resp = requests.get(urlk)
    data1 = resp.json()
    tickerlist = pd.json_normalize(data1,errors='ignore')
    
    # select Stock Tickers from the list
    stocktickerlist = tickerlist[(tickerlist['exch_seg'] == 'NSE') & (tickerlist['symbol'].str.contains('-EQ', regex = True) == True)]
    
    # Join the tables
    tickerAll = pd.merge(stocktickerlist, niftydf, left_on=['name'], right_on=['Symbol'], how='inner')
    
    
    # drop columns
    tickerAll.drop(tickerAll.columns[[1,3,4,5,6,8,9,10,11,12,13]], axis=1, inplace=True)
    
    
    
    ####generate session #####
    
    obj =SmartConnect(api_key="yourkey")
    login = obj.generateSession('clientID', 'password')
    refreshToken = login['data']['refreshToken']
    feedToken = obj.getfeedToken()
    
    #################################################
    
    candledf = pd.DataFrame()
    # histfinal_df = pd.DataFrame()
       
    # ------------------------------------ get Open, high, Low, Close of the Option Tikcers
    #
    for index,row in tickerAll.iterrows():
                
        exchange = row['exch_seg']
        token = row['token']
        interval = 'ONE_DAY'
        start = dateStart
        end = dateEnd
            
        historicParam={"exchange": exchange,"symboltoken": token,"interval": interval,"fromdate":start ,"todate": end}
        df = obj.getCandleData(historicParam)
        histprices = pd.json_normalize(df, "data", meta=["status", "errorcode","message"])
        histprices = histprices.drop(['status','errorcode','message'], axis = 1)
        histprices['token'] = token
        histprices.columns = ['date','open','high','low','close','volume','token']
              
        #format date
        histprices['date'] = pd.to_datetime(histprices["date"])
        
        histprices["month"] = pd.DatetimeIndex(histprices['date']).month
        histprices["week"] = pd.DatetimeIndex(histprices['date']).week
        
        
        
        # prepare the daily table of candledf
        daydf = histprices.loc[histprices['date'] > currentdate]
        
        # Drop columns
        daydf.drop(daydf.columns[[2,3,5,7,8]], axis=1, inplace=True)
        # add columns
        daydf['type'] = "daily"
        # Align columns
        daydf = daydf[['token','type','date','open','close']]
        daydf.columns =['token','type','number','open','close']
        
        
        
        # filter months n weeks
        monthdf = histprices[['month']]
        monthdf = monthdf.drop_duplicates()
        
        weekdf = histprices[['week']]
        weekdf = weekdf.drop_duplicates()
        weekdf = weekdf[weekdf['week'] > int(todayweek)-6]
        
        for index,row in monthdf.iterrows():
            
            varmonth = row['month']
            
            monthdfselect = histprices[histprices['month'] == varmonth]
            
            mindf = pd.to_datetime(monthdfselect['date']).idxmin()
            monthdfselectmin = monthdfselect.loc[mindf]
            varopen = monthdfselectmin.iat[1]
            
            maxdf = pd.to_datetime(monthdfselect['date']).idxmax()
            monthdfselectmax = monthdfselect.loc[maxdf]
            varclose = monthdfselectmax.iat[4]
            
            # Make a final table
            candle={'token':[token],
              'type':['monthly'],
              'number':[varmonth],
              'open':[varopen],
              'close':[varclose]}
            candle1 = pd.DataFrame(candle)
            
            candledf = candledf.append(candle1).reset_index(drop=True)
            
        
        for index,row in weekdf.iterrows():
            
            varweek = row['week']
            
            weekdfselect = histprices[histprices['week'] == varweek]
            
            mindf1 = pd.to_datetime(weekdfselect['date']).idxmin()
            monthdfselectmin1 = weekdfselect.loc[mindf1]
            varopen1 = monthdfselectmin1.iat[1]
            
            maxdf1 = pd.to_datetime(weekdfselect['date']).idxmax()
            monthdfselectmax1 = weekdfselect.loc[maxdf1]
            varclose1 = monthdfselectmax1.iat[4]
            
            # Make a final table
            candle2={'token':[token],
              'type':['weekly'],
              'number':[varweek],
              'open':[varopen1],
              'close':[varclose1]}
            candle3 = pd.DataFrame(candle2)
            
            candledf = candledf.append(candle3).reset_index(drop=True)
    
        candledf = candledf.append(daydf).reset_index(drop=True)
    
        time.sleep(2)
    
    # drop columns
    tickerAll.drop(tickerAll.columns[[2]], axis=1, inplace=True)
    # Join the tables
    candledffinal = pd.merge(candledf, tickerAll, left_on=['token'], right_on=['token'], how='left')
    
    
    # # Export the data
    candledffinal.to_csv (r'{}\candledata_{}.csv'.format(outputfiles,today), index = None, header=True)
    
    return candledffinal
