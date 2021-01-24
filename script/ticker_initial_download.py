import config
import finnhub
import json
import pandas as pd
import requests

import nltk
nltk.download('words')
from nltk.corpus import words
word_list = words.words()
word_list = [i.lower() for i in word_list]

# define exchanges
exchanges = [
    'F',	# DEUTSCHE BOERSE AG
    # 'HK',	# HONG KONG EXCHANGES AND CLEARING LTD
    # 'L',	# LONDON STOCK EXCHANGE
    # 'SI',	# SINGAPORE EXCHANGE
    # 'SS',	# SHANGHAI STOCK EXCHANGE
    # 'SW',	# SWISS EXCHANGE
    # 'T',	# TOKYO STOCK EXCHANGE-TOKYO PRO MARKET
    'US'	# US exchanges
]

# Setup client
finnhub_client = finnhub.Client(api_key=config.finnhub_key)

# get list of ticker per exchange via finnhub API
exchanges_ticker_list = []
for exchange in exchanges:
    r = requests.get('https://finnhub.io/api/v1/stock/symbol?exchange='+exchange+'&token='+config.finnhub_key)
    exchanges_ticker_list.append(r.json())
    print(f'Exchange completed: {exchange}')

# convert to list of dicts by saving as json
with open('./data/ticker.json', 'w', encoding='utf-8') as js:
    json.dump(exchanges_ticker_list, js)

# iterate through list of list of dicts and extract ticker symbol
ticker_list = []
with open('./data/ticker.json') as json_file:
    jdata = json.load(json_file)
    for exchange in jdata:
        for ticker in exchange:
            ticker_list.append(ticker['symbol'])

print(f'total list: {len(ticker_list)}')
ticker_list = [i.replace('.F', '') for i in ticker_list]
ticker_list = list(set(ticker_list))
print(f'dedub list: {len(ticker_list)}')
ticker_list = [i for i in ticker_list if len(i) > 2]
print(f'length filtered list: {len(ticker_list)}')
ticker_list = [i for i in ticker_list if i.lower() not in word_list]
print(f'word filtered list: {len(ticker_list)}')

# filter and write to csv file
df = pd.DataFrame(ticker_list, columns=['ticker'])
df.to_csv('./data/ticker.csv', sep=';', index=False)
