import pandas as pd
import datetime as dt
import numpy as np
import time
from unidirectionalStrategy import uds_raw_data
import glob
import os.path
from getLocationCell import getIndexes
from smartapi import SmartConnect
import requests
from datetime import datetime


st=time.time()

# files
inputfile1 = r'C:\Users\sukhw\OneDrive\Documents\TradingStrategy\files\bull'
inputfile2 = r'C:\Users\sukhw\OneDrive\Documents\TradingStrategy\files\bear'
outputfiles = r'C:\Users\sukhw\OneDrive\Documents\TradingStrategy\files'


# month and week numbers
todays_date = datetime.now()
dateStart = todays_date.replace(hour=9,minute=0).strftime("%Y-%m-%d %H:%M")
dateEnd = todays_date.replace(hour=16,minute=0).strftime("%Y-%m-%d %H:%M")

todayweek = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%V")
todaymonth = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%m")
today = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%Y-%m-%d %H-%M")

# find the latest excel file from the shared drive
folder_path = r'{}*'.format(inputfile1)
file_type = '*csv'
files = glob.glob(folder_path + file_type)
max_file = max(files, key=os.path.getctime)
# save in dataframe
bull = pd.read_csv (r'{}'.format(max_file))

# find the latest excel file from the shared drive
folder_path1 = r'{}*'.format(inputfile2)
file_type1 = '*csv'
files1 = glob.glob(folder_path1 + file_type1)
max_file1 = max(files1, key=os.path.getctime)
# save in dataframe
bear = pd.read_csv (r'{}'.format(max_file1))

####generate session #####

obj =SmartConnect(api_key="apikey")
login = obj.generateSession('clientid', 'password')
refreshToken = login['data']['refreshToken']
feedToken = obj.getfeedToken()

#################################################

bulldf = pd.DataFrame()
beardf = pd.DataFrame()
   
# ------------------------------------ get Open, high, Low, Close of bull Tikcers
#
for index,row in bull.iterrows():
                
    exchange = 'NSE'
    token = row['token']
    interval = 'FIFTEEN_MINUTE'
    start = dateStart
    end = dateEnd
    support = row['pricesupport']
    name = row['name']
        
    liveParam={"exchange": exchange,"symboltoken": token,"interval": interval,"fromdate":start ,"todate": end}
    df = obj.getCandleData(liveParam)
    liveprices = pd.json_normalize(df, "data", meta=["status", "errorcode","message"])
    liveprices = liveprices.drop(['status','errorcode','message'], axis = 1)
    liveprices['token'] = token
    liveprices.columns = ['date','open','high','low','close','volume','token']
    
    # add candles category
    liveprices['candles'] = np.where (liveprices['open']> liveprices['close'],'red','green')

    # count dataframe rows
    num_rows = liveprices.count()[0]
    # drop latest candle
    liveprices1 = liveprices.head(num_rows-1)
      
    # find current stop and check the point
    currentspot = liveprices.iat[num_rows-1,4]
    if currentspot > support:
        spotpricecheck = 'trade'
    else:
        spotpricecheck = 'no-trade'
        
    # find green candles minimum of open
    try:
        # find green candles minimum of open
        green = liveprices1[(liveprices1['candles'] == 'green')]
        mindf = green['open'].idxmin()
        greenmin = green.loc[mindf]
        greenopen = greenmin.iat[1]
        
        # find green candles maximum of close
        maxdf = green['close'].idxmax()
        greenmax = green.loc[maxdf]
        greenclose = greenmax.iat[4]
    except:
        greenopen = 0
        greenclose = 0
    
    try:
        # find red candles maximum of open
        red = liveprices1[(liveprices1['candles'] == 'red')]
        maxdf = red['open'].idxmax()
        redmax = red.loc[maxdf]
        redopen = redmax.iat[1]
        
        # find red candles minimum of close
        mindf = red['close'].idxmin()
        redmin = red.loc[mindf]
        redclose = redmin.iat[4]
    except:
        redopen = 0
        redclose = 1000000
        
    # current trend logic
    if liveprices1.iat[0,7] == 'green':
        if redclose > greenopen:
            currenttrend = 'bull'
        else:
            currenttrend = 'no-trade'
    else:
        if greenclose > redopen:
            currenttrend = 'bull'
        else:
            currenttrend = 'no-trade'
    
    # find stop loss
    stoploss = greenopen

    
    # Make a final table
    currenttrenddf = {'name':[name],
              'token':[token],
              'firstcandle':[liveprices1.iat[0,7]],
              'greenopen':[greenopen],
              'greenclose':[greenclose],
              'redopen':[redopen],
              'redclose':[redclose],
              'currenttrend':[currenttrend],
              'support':[support],
              'currentspot':[currentspot],
              'spotpricecheck':[spotpricecheck],
              'stoploss':[stoploss]}
    currenttrend_df = pd.DataFrame(currenttrenddf)
    bulldf = bulldf.append(currenttrend_df).reset_index(drop=True)
    time.sleep(0.2)

# filter table for only bull trades
currentbulldf = bulldf[(bulldf['currenttrend'] == 'bull') & (bulldf['spotpricecheck'] == 'trade')]
# add profit making column
# currentbulldf['profit'] = (currentbulldf['currentspot'] * 0.01) + currentbulldf['currentspot']



# ------------------------------------ get Open, high, Low, Close of bull Tikcers

#
for index,row in bear.iterrows():
                
    exchange = 'NSE'
    token = row['token']
    interval = 'FIFTEEN_MINUTE'
    start = dateStart
    end = dateEnd
    support = row['priceresistance']
    name = row['name']
        
    liveParam={"exchange": exchange,"symboltoken": token,"interval": interval,"fromdate":start ,"todate": end}
    df = obj.getCandleData(liveParam)
    liveprices = pd.json_normalize(df, "data", meta=["status", "errorcode","message"])
    liveprices = liveprices.drop(['status','errorcode','message'], axis = 1)
    liveprices['token'] = token
    liveprices.columns = ['date','open','high','low','close','volume','token']
    
    # add candles category
    liveprices['candles'] = np.where (liveprices['open']> liveprices['close'],'red','green')

    # count dataframe rows
    num_rows = liveprices.count()[0]
    # drop latest candle
    liveprices1 = liveprices.head(num_rows-1)
      
    # find current stop and check the point
    currentspot = liveprices.iat[num_rows-1,4]
    if currentspot < support:
        spotpricecheck = 'trade'
    else:
        spotpricecheck = 'no-trade'
        
    # find green candles minimum of open
    try:
        # find green candles minimum of open
        green = liveprices1[(liveprices1['candles'] == 'green')]
        mindf = green['open'].idxmin()
        greenmin = green.loc[mindf]
        greenopen = greenmin.iat[1]
        
        # find green candles maximum of close
        maxdf = green['close'].idxmax()
        greenmax = green.loc[maxdf]
        greenclose = greenmax.iat[4]
    except:
        greenopen = 0
        greenclose = 0
    
    try:
        # find red candles maximum of open
        red = liveprices1[(liveprices1['candles'] == 'red')]
        maxdf = red['open'].idxmax()
        redmax = red.loc[maxdf]
        redopen = redmax.iat[1]
        
        # find red candles minimum of close
        mindf = red['close'].idxmin()
        redmin = red.loc[mindf]
        redclose = redmin.iat[4]
    except:
        redopen = 0
        redclose = 1000000
        
    # current trend logic
    if liveprices1.iat[0,7] == 'green':
        if redclose > greenopen:
            currenttrend = 'no-trade'
        else:
            currenttrend = 'bear'
    else:
        if greenclose > redopen:
            currenttrend = 'no-trade'
        else:
            currenttrend = 'bear'
    
    # find stop loss
    stoploss = redopen

    
    # Make a final table
    currenttrendbeardf = {'name':[name],
              'token':[token],
              'firstcandle':[liveprices1.iat[0,7]],
              'greenopen':[greenopen],
              'greenclose':[greenclose],
              'redopen':[redopen],
              'redclose':[redclose],
              'currenttrend':[currenttrend],
              'support':[support],
              'currentspot':[currentspot],
              'spotpricecheck':[spotpricecheck],
              'stoploss':[stoploss]}
    currenttrendbear_df = pd.DataFrame(currenttrendbeardf)
    beardf = beardf.append(currenttrendbear_df).reset_index(drop=True)
    time.sleep(0.2)

# filter table for only bull trades
currentbeardf = beardf[(beardf['currenttrend'] == 'bear') & (beardf['spotpricecheck'] == 'trade')]
# add profit making column
# currentbeardf['profit'] = currentbeardf['currentspot'] - (currentbeardf['currentspot'] * 0.01)  


# # # # Export the data
currentbulldf.to_excel (r'{}\Currentbull_{}.xlsx'.format(outputfiles,today), index = None, header=True)
currentbeardf.to_excel (r'{}\Currentbear_{}.xlsx'.format(outputfiles,today), index = None, header=True)

et=time.time()
print("run time: %g Minutes" % ((et-st)/60))






