'''
Python script to crawl submissions and all their comments from specified
subreddits using reddits praw API
'''

import config
from datetime import datetime
from datetime import date
import pandas as pd
import pyodbc
import praw

# config vars
current_Time = datetime.now()
current_date = date.today().strftime(r"%Y%m%d")

submission_limit = 15 # set number of submissions crawled per subreddit

conn = pyodbc.connect(config.sql_connection_string)
cursor = conn.cursor()
cursor.fast_executemany = True

# get existing comment ids already in db
exist_comment_ids = []

cursor.execute('SELECT comment_id FROM comment_table')
for row in cursor.fetchall():
    exist_comment_ids.append(row[0])

# set reddit instance
reddit = praw.Reddit(
    client_id = config.client_id
    , client_secret = config.client_secret
    , user_agent = config.user_agent
)
# define subreddits to be crawled
subs = ['wallstreetbets'] # , 'stocks', 'Finanzen', 'mauerstrassenwetten', 'options']

# define dataframe for comments meta data + comment body
col_header = [
    'subreddit', 'comment_id', 'created_utc', 'body'
]
df_crawled = pd.DataFrame(columns=col_header)
new_comment_ids = []

for s in subs:
    for submission in reddit.subreddit(s).new(limit=submission_limit):
        submission.comments.replace_more(limit=None) # get CommentForest
        for comment in submission.comments.list():
            
            # check if comment id already crawled previously (existing in db)
            if comment.id not in exist_comment_ids:
                # initialize list and append meta data + body
                comments_list = []
                comments_list.append(s)
                comments_list.append(comment.id)
                comments_list.append(
                    datetime.utcfromtimestamp(comment.created_utc).date()
                )
                comments_list.append(
                    comment.body.lower()
                )

                # save comment to df_crawled
                temp_df_crawled = pd.DataFrame([comments_list], columns=col_header)
                df_crawled = df_crawled.append(temp_df_crawled)
                new_comment_ids.append(comment.id)

df_crawled.reset_index(drop=True, inplace=True)

# connect to sql and write comments IDs to db
new_comment_ids = list(set(new_comment_ids))
new_comment_ids = [[item] for item in new_comment_ids]

sql_header = 'comment_id'
sql_comment_insertion = f'INSERT INTO {config.sql_database}.dbo.{config.sql_table_comments} ({sql_header}) values(?)'

if len(new_comment_ids) > 0:
    cursor.executemany(sql_comment_insertion, new_comment_ids)
    conn.commit()
    print(f'successful: {len(new_comment_ids)} new comment IDs added to db')
else:
    print('no new comments')

# filter for ticker and count mentions
all_ticker = []

cursor.execute('SELECT ticker FROM ticker_table')
for row in cursor.fetchall():
    all_ticker.append(row[0])

ticker_found = [] # schema to be: 'subreddit', 'crawl_date', 'ticker', 'amount'

for index, row in df_crawled.iterrows():
    body = row['body'] \
        .replace('.', '') \
        .replace(',', '') \
        .replace('/', '') \
        .replace('*', '') \
        .replace('_', '') \
        .replace('-', '') \
        .replace('?', '') \
        .replace('!', '') \
        .replace('(', '') \
        .replace(')', '') \
        .replace('[', '') \
        .replace(']', '') \
        .replace(':', '')
    body = body.split()
    for ticker in all_ticker:
        if ticker.lower() in body:
            ticker_found.append([row['subreddit'], row['created_utc'], ticker, 1])

rsa_header = ['subreddit', 'crawl_date', 'ticker', 'amount']
df_ticker_found = pd.DataFrame(ticker_found
    , columns = rsa_header
)
df_ticker_found = df_ticker_found.groupby(['subreddit', 'crawl_date', 'ticker']) \
    .agg({'amount': ['sum']})
df_ticker_found.reset_index(inplace=True)
df_ticker_found.columns = rsa_header

# check with existing ticker counts and update
df_ticker_exist = pd.read_sql('SELECT * FROM rsa_table', conn)

df_ticker_comb = pd.concat([df_ticker_exist, df_ticker_found])
df_ticker_comb = df_ticker_comb.groupby(['subreddit', 'crawl_date', 'ticker']) \
    .agg({'amount': ['sum']})
df_ticker_comb.reset_index(inplace=True)
df_ticker_comb.columns = rsa_header

# update db
# TODO: currently only inserted into tabel
# TODO: check aggregation
rsa_upload = df_ticker_comb.values.tolist()
sql_rsa_insertion = f'INSERT INTO {config.sql_database}.dbo.{config.sql_table_rsa} ({rsa_header[0]}, {rsa_header[1]}, {rsa_header[2]}, {rsa_header[3]}) values(?,?,?,?)'
cursor.executemany(sql_rsa_insertion, rsa_upload)
conn.commit()
