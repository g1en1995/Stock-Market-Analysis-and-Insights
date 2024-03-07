import bs4 as bs
import pandas as pd
import numpy as np
import requests 
import pickle
import re
from time import sleep
from json import dumps
import json
from datetime import date, timedelta
import yfinance as yf

market = []
resp = requests.get('https://stockanalysis.com/list/sp-500-stocks/')
soup = bs.BeautifulSoup(resp.text, 'lxml')
table = soup.find('table', {'id':'main-table'})
ticker = []
for row in table.findAll('tr', {'class':'svelte-cod2gs'}):
    company = []
    for each in row.findAll('td'):
        company.append(each.text.strip())
    market.append(company)
    
df = pd.DataFrame(market)
ticker= df[1:][1]

def get_daily_stock_data(tickers):
    yesterday =  str(date.today() + timedelta(days = -1))
    today =  str(date.today())

    # dataframe for all the tickers
    stock_data = pd.DataFrame()
    for ticker in tickers:
        print(ticker)
        # download data
        data = yf.download(tickers = ticker, start = yesterday, end = today, progress=False)

        # add a column for ticker
        data = data.assign(stock_index = ticker)

        # add date column
        data = data.assign(date = data.index)

        # append this ticker data to larger dataframe
        stock_data = pd.concat([stock_data, data])

    return stock_data

data = get_daily_stock_data(ticker)

# For apache kafka to send the data through producer the data needs to be json serializable
print(data.to_json(orient = 'records'))



