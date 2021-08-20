import pandas as pd
import datetime as dt
import numpy as np
import time
from unidirectionalStrategy import uds_raw_data
import glob
import os.path
from getLocationCell import getIndexes


st=time.time()

# month and week numbers
todayweek = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%V")
todaymonth = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%m")
today = (dt.datetime.today() - dt.timedelta(days=0)).strftime("%Y-%m-%d")


# get the data
candledffinal = uds_raw_data()

# Candle file generated from Candledffinal function
# find the latest excel file from the shared drive
folder_path = r'C:\Users\sukhw\OneDrive\Documents\TradingStrategy\files\candle*'
file_type = '*csv'
files = glob.glob(folder_path + file_type)
max_file = max(files, key=os.path.getctime)
# save in dataframe
candledffinal = pd.read_csv (r'{}'.format(max_file))

# define color of candle
candledffinal['color'] = np.where(candledffinal['open'] > candledffinal['close'], 'red','green')

# make a table for all token list
tickerdf = candledffinal[['token','type']]
tickerdf = tickerdf.drop_duplicates(keep='first')

# make monthly table
monthlytickerdf = tickerdf[(tickerdf['type'] == 'monthly')]

trenddfinal = pd.DataFrame()

for index,row in monthlytickerdf.iterrows():
    
    vartoken = row['token']
    vartype = row['type']
    
    # select Stock Tickers from the list
    monthdf = candledffinal[(candledffinal['type'] == vartype) & (candledffinal['token'] == vartoken)]
    
    # count dataframe rows
    num_rows = monthdf.count()[0]
    
    if num_rows >= 5:
        # sort
        monthdf = monthdf.sort_values(by = ['number'], ascending = [False]).reset_index(drop=True)
        # filter rows
        monthdf1 = monthdf.iloc[[1,2,3,4]]
        
        listOfElems = ['green', 'red']
        dictOfPos = {elem: getIndexes(monthdf1, elem) for elem in listOfElems}
        greenaddressdf = pd.json_normalize(dictOfPos,"green")
        redaddressdf = pd.json_normalize(dictOfPos,"red")
        try:
            radd = redaddressdf.iat[0,0]-1
        except:
            radd = greenaddressdf.iat[0,0]
            
        try:
            gadd = greenaddressdf.iat[0,0]-1
        except:
            gadd = redaddressdf.iat[0,0]
                
        # define variables
        name = monthdf1.iat[0,5]
        token = monthdf1.iat[0,0]
        candle = monthdf1.iat[0,1]
        if radd < gadd:
            firstopen = monthdf1.iat[radd,3]
            firstclose = monthdf1.iat[radd,4]
            firstcolor = monthdf1.iat[radd,6]
            secondopen = monthdf1.iat[gadd,3]
            secondclose = monthdf1.iat[gadd,4]
            secondcolor = monthdf1.iat[gadd,6]
        else:
            firstopen = monthdf1.iat[gadd,3]
            firstclose = monthdf1.iat[gadd,4]
            firstcolor = monthdf1.iat[gadd,6]
            secondopen = monthdf1.iat[radd,3]
            secondclose = monthdf1.iat[radd,4]
            secondcolor = monthdf1.iat[radd,6]
    
        # Make a final table
        trend = {'name':[name],
                  'token':[token],
                  'candle':[candle],
                  'firstopen':[firstopen],
                  'firstclose':[firstclose],
                  'firstcolor':[firstcolor],
                  'secondopen':[secondopen],
                  'secondclose':[secondclose],
                  'secondcolor':[secondcolor]}
        trenddf = pd.DataFrame(trend)
        trenddf['trend'] = np.where((trenddf['firstcolor']=='green') & (trenddf['secondcolor']=='red') & (trenddf['firstclose'] > trenddf['secondopen']), 'bull',
                            np.where((trenddf['firstcolor']=='green') & (trenddf['secondcolor']=='red') & (trenddf['firstclose'] < trenddf['secondopen']), 'bear',
                              np.where((trenddf['firstcolor']=='green') & (trenddf['secondcolor']=='green'), 'bull',
                              np.where((trenddf['firstcolor']=='red') & (trenddf['secondcolor']=='green') & (trenddf['firstclose'] < trenddf['secondopen']), 'bear',
                                np.where((trenddf['firstcolor']=='red') & (trenddf['secondcolor']=='green') & (trenddf['firstclose'] > trenddf['secondopen']), 'bull', 'bear')))))    
        
        trenddfinal = trenddfinal.append(trenddf).reset_index(drop=True)


# # make weekly table
weeklytickerdf = tickerdf[(tickerdf['type'] == 'weekly')]


for index,row in weeklytickerdf.iterrows():
    
    vartoken = row['token']
    vartype = row['type']
    
    # select Stock Tickers from the list
    weekdf = candledffinal[(candledffinal['type'] == vartype) & (candledffinal['token'] == vartoken)]
    
    # count dataframe rows
    num_rows = weekdf.count()[0]
    
    if num_rows >= 5:
        # sort
        weekdf = weekdf.sort_values(by = ['number'], ascending = [False]).reset_index(drop=True)
        # filter rows
        weekdf1 = weekdf.iloc[[1,2,3,4]]
        
        listOfElems = ['green', 'red']
        dictOfPos = {elem: getIndexes(weekdf1, elem) for elem in listOfElems}
        greenaddressdf = pd.json_normalize(dictOfPos,"green")
        redaddressdf = pd.json_normalize(dictOfPos,"red")
        try:
            radd = redaddressdf.iat[0,0]-1
        except:
            radd = greenaddressdf.iat[0,0]
            
        try:
            gadd = greenaddressdf.iat[0,0]-1
        except:
            gadd = redaddressdf.iat[0,0]

            
        # define variables
        name = weekdf1.iat[0,5]
        token = weekdf1.iat[0,0]
        candle = weekdf1.iat[0,1]
        if radd < gadd:
            firstopen = weekdf1.iat[radd,3]
            firstclose = weekdf1.iat[radd,4]
            firstcolor = weekdf1.iat[radd,6]
            secondopen = weekdf1.iat[gadd,3]
            secondclose = weekdf1.iat[gadd,4]
            secondcolor = weekdf1.iat[gadd,6]
        else:
            firstopen = weekdf1.iat[gadd,3]
            firstclose = weekdf1.iat[gadd,4]
            firstcolor = weekdf1.iat[gadd,6]
            secondopen = weekdf1.iat[radd,3]
            secondclose = weekdf1.iat[radd,4]
            secondcolor = weekdf1.iat[radd,6]
        
        
        # Make a final table
        trend1 = {'name':[name],
                  'token':[token],
                  'candle':[candle],
                  'firstopen':[firstopen],
                  'firstclose':[firstclose],
                  'firstcolor':[firstcolor],
                  'secondopen':[secondopen],
                  'secondclose':[secondclose],
                  'secondcolor':[secondcolor]}
        trenddf1 = pd.DataFrame(trend1)
        trenddf1['trend'] = np.where((trenddf1['firstcolor']=='green') & (trenddf1['secondcolor']=='red') & (trenddf1['firstclose'] > trenddf1['secondopen']), 'bull',
                            np.where((trenddf1['firstcolor']=='green') & (trenddf1['secondcolor']=='red') & (trenddf1['firstclose'] < trenddf1['secondopen']), 'bear',
                              np.where((trenddf1['firstcolor']=='green') & (trenddf1['secondcolor']=='green'), 'bull',
                              np.where((trenddf1['firstcolor']=='red') & (trenddf1['secondcolor']=='green') & (trenddf1['firstclose'] < trenddf1['secondopen']), 'bear',
                                np.where((trenddf1['firstcolor']=='red') & (trenddf1['secondcolor']=='green') & (trenddf1['firstclose'] > trenddf1['secondopen']), 'bull', 'bear')))))    
        
        trenddfinal = trenddfinal.append(trenddf1).reset_index(drop=True)


# # make daily table
dailytickerdf = tickerdf[(tickerdf['type'] == 'daily')]


for index,row in dailytickerdf.iterrows():
    
    vartoken = row['token']
    vartype = row['type']
    
    # select Stock Tickers from the list
    dailydf = candledffinal[(candledffinal['type'] == vartype) & (candledffinal['token'] == vartoken)]
    
    # count dataframe rows
    num_rows = dailydf.count()[0]
    
    if num_rows >= 5:
        # sort
        dailydf = dailydf.sort_values(by = ['number'], ascending = [False]).reset_index(drop=True)
        # filter rows
        dailydf1 = dailydf.iloc[[0,1,2,3,4]]
        
        listOfElems = ['green', 'red']
        dictOfPos = {elem: getIndexes(dailydf1, elem) for elem in listOfElems}
        greenaddressdf = pd.json_normalize(dictOfPos,"green")
        redaddressdf = pd.json_normalize(dictOfPos,"red")
        try:
            radd = redaddressdf.iat[0,0]
        except:
            radd = greenaddressdf.iat[0,0]+1
            
        try:
            gadd = greenaddressdf.iat[0,0]
        except:
            gadd = redaddressdf.iat[0,0]+1
    
            
        # define variables
        name = dailydf1.iat[0,5]
        token = dailydf1.iat[0,0]
        candle = dailydf1.iat[0,1]
        if radd < gadd:
            firstopen = dailydf1.iat[radd,3]
            firstclose = dailydf1.iat[radd,4]
            firstcolor = dailydf1.iat[radd,6]
            secondopen = dailydf1.iat[gadd,3]
            secondclose = dailydf1.iat[gadd,4]
            secondcolor = dailydf1.iat[gadd,6]
        else:
            firstopen = dailydf1.iat[gadd,3]
            firstclose = dailydf1.iat[gadd,4]
            firstcolor = dailydf1.iat[gadd,6]
            secondopen = dailydf1.iat[radd,3]
            secondclose = dailydf1.iat[radd,4]
            secondcolor = dailydf1.iat[radd,6]
    
        
        # Make a final table
        trend2 = {'name':[name],
                  'token':[token],
                  'candle':[candle],
                  'firstopen':[firstopen],
                  'firstclose':[firstclose],
                  'firstcolor':[firstcolor],
                  'secondopen':[secondopen],
                  'secondclose':[secondclose],
                  'secondcolor':[secondcolor]}
        trenddf2 = pd.DataFrame(trend2)
        trenddf2['trend'] = np.where((trenddf2['firstcolor']=='green') & (trenddf2['secondcolor']=='red') & (trenddf2['firstclose'] > trenddf2['secondopen']), 'bull',
                            np.where((trenddf2['firstcolor']=='green') & (trenddf2['secondcolor']=='red') & (trenddf2['firstclose'] < trenddf2['secondopen']), 'bear',
                              np.where((trenddf2['firstcolor']=='green') & (trenddf2['secondcolor']=='green'), 'bull',
                              np.where((trenddf2['firstcolor']=='red') & (trenddf2['secondcolor']=='green') & (trenddf2['firstclose'] < trenddf2['secondopen']), 'bear',
                                np.where((trenddf2['firstcolor']=='red') & (trenddf2['secondcolor']=='green') & (trenddf2['firstclose'] > trenddf2['secondopen']), 'bull', 'bear')))))    
        
        trenddfinal = trenddfinal.append(trenddf2).reset_index(drop=True)
    


# make a table for all token list
trenddfinal1 = trenddfinal[['token']]
trenddfinal1 = trenddfinal1.drop_duplicates(keep='first')

logicfinal = pd.DataFrame()

# decision making logic
for index,row in trenddfinal1.iterrows():

    vartoken = row['token']
    
    # select Stock Tickers from the list
    logicdf = trenddfinal[(trenddfinal['token'] == vartoken)]
    
    # count dataframe rows
    num_rows = logicdf.count()[0]
    
    if num_rows >= 3:
            
        # define variables
        name = logicdf.iat[0,0]
        token = logicdf.iat[0,1]
        monthlytrend = logicdf.iat[0,9]
        weeklytrend = logicdf.iat[1,9]
        dailytrend = logicdf.iat[2,9]
        if logicdf.iat[2,5] == 'red':
            priceresistance = logicdf.iat[2,3]
        else:
            priceresistance = logicdf.iat[2,6]
            
        if logicdf.iat[2,5] == 'green':
            pricesupport = logicdf.iat[2,3]
        else:
            pricesupport = logicdf.iat[2,6]

       
        # Make a final table
        logicdf1 = {'name':[name],
                  'token':[token],
                  'monthlytrend':[monthlytrend],
                  'weeklytrend':[weeklytrend],
                  'dailytrend':[dailytrend],
                  'priceresistance':[priceresistance],
                  'pricesupport':[pricesupport]}
        logicdf2 = pd.DataFrame(logicdf1)
        logicdf2['finaltrend'] = np.where((logicdf2['monthlytrend']=='bull') & (logicdf2['weeklytrend']=='bull') & (logicdf2['dailytrend']== 'bull'), 'bull',
                                    np.where((logicdf2['monthlytrend']=='bear') & (logicdf2['weeklytrend']=='bear') & (logicdf2['dailytrend']== 'bear'), 'bear','no trade'))
        logicfinal = logicfinal.append(logicdf2).reset_index(drop=True)

# select bull and bear trend names
bulldf = logicfinal[(logicfinal['finaltrend'] == 'bull')]
bulldf.drop(bulldf.columns[[5]], axis=1, inplace=True)

beardf = logicfinal[(logicfinal['finaltrend'] == 'bear')]
beardf.drop(beardf.columns[[6]], axis=1, inplace=True)

# # # Export the data
beardf.to_csv (r'C:\Users\sukhw\OneDrive\Documents\TradingStrategy\files\bear_{}.csv'.format(today), index = None, header=True)
bulldf.to_csv (r'C:\Users\sukhw\OneDrive\Documents\TradingStrategy\files\bull_{}.csv'.format(today), index = None, header=True)

et=time.time()
print("run time: %g Minutes" % ((et-st)/60))






