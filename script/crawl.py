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

comment_header = ['subreddit', 'comment_id', 'created_utc', 'body']
rsa_header = ['subreddit', 'crawl_date', 'ticker', 'amount']

# get existing comment ids already in db
exist_comment_ids = []

cursor.execute(f'SELECT {comment_header[1]} FROM {config.sql_database}.dbo.{config.sql_table_comments}')
for row in cursor.fetchall():
    exist_comment_ids.append(row[0])

# set reddit instance
reddit = praw.Reddit(
    client_id = config.client_id
    , client_secret = config.client_secret
    , user_agent = config.user_agent
)
# define subreddits to be crawled
subs = ['Finanzen'] # , 'wallstreetbets', 'stocks', 'Finanzen', 'mauerstrassenwetten', 'options']

# define dataframe for comments meta data + comment body
df_crawled = pd.DataFrame(columns=comment_header)
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
                    comment.body
                )

                # save comment to df_crawled
                temp_df_crawled = pd.DataFrame([comments_list], columns=comment_header)
                df_crawled = df_crawled.append(temp_df_crawled)
                new_comment_ids.append(comment.id)

df_crawled.reset_index(drop=True, inplace=True)

# connect to sql and write comments IDs to db
new_comment_ids = list(set(new_comment_ids))
new_comment_ids = [[item] for item in new_comment_ids]

if len(new_comment_ids) > 0:
    sql_comment_insertion = f"""
        INSERT INTO {config.sql_database}.dbo.{config.sql_table_comments}
        ({comment_header[1]}) values(?)
    """
    cursor.executemany(sql_comment_insertion, new_comment_ids)
    cursor.commit()
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
        if ticker in body:
            ticker_found.append([row['subreddit'], row['created_utc'], ticker, 1])

# transform ticker found to df to aggregate
if len(ticker_found) > 0:
    df_ticker_found = pd.DataFrame(ticker_found, columns = rsa_header)
    df_ticker_found = df_ticker_found \
        .groupby(['subreddit', 'crawl_date', 'ticker']) \
        .agg({'amount': ['sum']})
    df_ticker_found.reset_index(inplace=True)
    df_ticker_found.columns = rsa_header

    new_rows = []

    # check if row (subreddit, crawl_date, ticker) already on db an merge
    for index, row in df_ticker_found.iterrows():
        sql_rsa_check = f"""
            SELECT {rsa_header[3]} FROM {config.sql_database}.dbo.{config.sql_table_rsa} 
            WHERE
                {rsa_header[0]} = '{row[0]}' AND
                {rsa_header[1]} = '{row[1]}' AND
                {rsa_header[2]} = '{row[2]}'
            """

        cursor.execute(sql_rsa_check)
        rsa_check = cursor.fetchone()
        if rsa_check == None:
            rsa_check = 0
        else:
            rsa_check = rsa_check[0]

        if rsa_check > 0:
            sql_rsa_update = f"""
                UPDATE {config.sql_database}.dbo.{config.sql_table_rsa}
                SET {rsa_header[3]} = ({rsa_check} + {row[3]})
                WHERE
                    {rsa_header[0]} = '{row[0]}' AND
                    {rsa_header[1]} = '{row[1]}' AND
                    {rsa_header[2]} = '{row[2]}'
            """
            cursor.execute(sql_rsa_update)
            cursor.commit()
        else:
            new_rows.append(row.tolist())
    if len(new_rows) > 0:
        sql_rsa_insertion = f"""
            INSERT INTO {config.sql_database}.dbo.{config.sql_table_rsa} 
            ({rsa_header[0]}, {rsa_header[1]}, {rsa_header[2]}, {rsa_header[3]}) 
            values(?,?,?,?)
        """
        cursor.executemany(sql_rsa_insertion, new_rows)
        conn.commit()
else:
    print('no new ticker')
