import config
import pandas as pd
import pyodbc

df = pd.read_csv('ticker.csv', sep=';', encoding = "ISO-8859-1")

# connect to sql and write to db
sql_header = 'ticker, company, exchange, country'
sql_insertion = f'INSERT INTO {config.sql_database}.dbo.{config.sql_table_ticker} ({sql_header}) values(?,?,?,?)'

with pyodbc.connect(config.sql_connection_string) as conn:
    with conn.cursor() as cursor:
        counter = 0
        for index, row in df.iterrows():
            if counter % 100 == 0:
                print(counter)
            cursor.execute(
                sql_insertion,
                row.ticker,
                row.company,
                row.exchange,
                row.country
            )
            counter += 1

    conn.commit()
    cursor.close()
