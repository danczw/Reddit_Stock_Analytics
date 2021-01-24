import config
from nltk.corpus import words
import pandas as pd
import pyodbc

df = pd.read_csv('./data/ticker.csv', sep=';')

ticker = list(df['ticker'])
ticker = [[item] for item in ticker]

# connect to sql and write to db
sql_header = 'ticker'
sql_insertion = f'INSERT INTO {config.sql_database}.dbo.{config.sql_table_ticker} ({sql_header}) values(?)'

with pyodbc.connect(config.sql_connection_string) as conn:
    with conn.cursor() as cursor:
        cursor.fast_executemany = True
        cursor.executemany(sql_insertion, ticker)
        # for index, row in df.iterrows():
        #     cursor.execute(
        #         sql_insertion,
        #         row.ticker
        #     )

    conn.commit()
    cursor.close()
