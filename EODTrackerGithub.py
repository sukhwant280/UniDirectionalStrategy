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

profitmaking = 0.005 # change for results which suits you. its a %

# month and week numbers
today = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%Y-%m-%d")
todays_date = datetime.now()
dateStart = todays_date.replace(hour=9,minute=0).strftime("%Y-%m-%d %H:%M")
dateEnd = todays_date.replace(hour=16,minute=0).strftime("%Y-%m-%d %H:%M")
todayweek = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%V")
todaymonth = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%m")

# find the latest excel file from the shared drive
folder_path = r'C:\Users\sukhw\OneDrive\Documents\TradingStrategy\files\Currentbull_*'
file_type = '*xlsx'
files = glob.glob(folder_path + file_type)
max_file = max(files, key=os.path.getctime)
# save in dataframe
bull = pd.read_excel (r'{}'.format(max_file))
# add profit making column
bull['profit'] = bull['currentspot'] + (bull['currentspot'] * profitmaking)  
bull['result'] = '' 
bull['high'] = ''
bull['low'] = ''


# find the latest excel file from the shared drive
folder_path1 = r'C:\Users\sukhw\OneDrive\Documents\TradingStrategy\files\Currentbear_*'
file_type1 = '*xlsx'
files1 = glob.glob(folder_path1 + file_type1)
max_file1 = max(files1, key=os.path.getctime)
# save in dataframe
bear = pd.read_excel (r'{}'.format(max_file1))
# add profit making column
bear['profit'] = bear['currentspot'] - (bear['currentspot'] * profitmaking)  
bear['result'] = '' 
bear['high'] = ''
bear['low'] = ''


####generate session #####

obj =SmartConnect(api_key="yourapikey")
login = obj.generateSession('clientid', 'password')
refreshToken = login['data']['refreshToken']
feedToken = obj.getfeedToken()

#################################################

# ------------------------------------ get Open, high, Low, Close of bull Tikcers
a = 0
for index,row in bull.iterrows():
                        
    exchange = 'NSE'
    token = row['token']
    interval = 'FIFTEEN_MINUTE'
    start = dateStart
    end = dateEnd
    stoploss = row['stoploss']
    profit = row['profit']
    currentspot = row['currentspot']
     
       
    liveParam={"exchange": exchange,"symboltoken": token,"interval": interval,"fromdate":start ,"todate": end}
    df = obj.getCandleData(liveParam)
    liveprices = pd.json_normalize(df, "data", meta=["status", "errorcode","message"])
    liveprices = liveprices.drop(['status','errorcode','message'], axis = 1)
    liveprices['token'] = token
    liveprices.columns = ['date','open','high','low','close','volume','token']
    
    # sort table
    liveprices = liveprices.sort_values(by='date', ascending=False)
    
    # drop latest candle
    liveprices1 = liveprices.head(22)
    
    # sort table
    liveprices1 = liveprices1.sort_values(by='date', ascending=True)
         
    for index,row in liveprices1.iterrows():
    
        high = row['high']
        low = row['low']
    
        result = np.where( high > profit, 'win',
                  np.where( low < stoploss, 'loss',
                    np.where((a == 21), 'win','no trade')))
        if result == 'win' or result =='loss':
            break
     
    bull.iat[a,13] = result
    bull.iat[a,14] = high
    bull.iat[a,15] = low
    
    a = a+1
    time.sleep(2)
    
    bull['daterecord'] = today



# ------------------------------------ get Open, high, Low, Close of bear Tikcers
               
a = 0
for index,row in bear.iterrows():
                    
    exchange = 'NSE'
    token = row['token']
    interval = 'FIFTEEN_MINUTE'
    start = dateStart
    end = dateEnd
    stoploss = row['stoploss']
    profit = row['profit']
    currentspot = row['currentspot']
     
       
    liveParam={"exchange": exchange,"symboltoken": token,"interval": interval,"fromdate":start ,"todate": end}
    df = obj.getCandleData(liveParam)
    liveprices = pd.json_normalize(df, "data", meta=["status", "errorcode","message"])
    liveprices = liveprices.drop(['status','errorcode','message'], axis = 1)
    liveprices['token'] = token
    liveprices.columns = ['date','open','high','low','close','volume','token']
    
    # sort table
    liveprices = liveprices.sort_values(by='date', ascending=False)
    
    # drop latest candle
    liveprices1 = liveprices.head(22)
    
    # sort table
    liveprices1 = liveprices1.sort_values(by='date', ascending=True)
         
    for index,row in liveprices1.iterrows():
    
        high = row['high']
        low = row['low']
    
        result = np.where( low < profit, 'win',
                  np.where( high > stoploss, 'loss',
                    np.where((a == 21), 'win','no trade')))
        if result == 'win' or result =='loss':
            break
     
    bear.iat[a,13] = result
    bear.iat[a,14] = high
    bear.iat[a,15] = low
    
    a = a+1
    time.sleep(2)

bear['daterecord'] = today


# # # # Export the data
bull.to_excel (r'C:\Users\sukhw\OneDrive\Documents\TradingStrategy\files\TrackerBull_{}.xlsx'.format(today), index = None, header=True)
bear.to_excel (r'C:\Users\sukhw\OneDrive\Documents\TradingStrategy\files\TrackerBear_{}.xlsx'.format(today), index = None, header=True)

et=time.time()
print("run time: %g Minutes" % ((et-st)/60))






