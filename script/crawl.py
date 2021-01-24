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
submission_limit = 2 # set number of submissions crawled per subreddit

# get existing comment ids already in db
exist_comment_ids = []
with pyodbc.connect(config.sql_connection_string) as conn:
    with conn.cursor() as cursor:
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
subs = ['wallstreetbets'] # , 'stocks', 'Finanzen', 'mauerstrassenwetten'

# define dataframe for comments meta data + comment body
col_header = [
    'subreddit', 'comment_id', 'created_utc', 'body'
]
df_crawled = pd.DataFrame(columns=col_header)
new_comment_ids = []

for s in subs:
    for submission in reddit.subreddit(s).hot(limit=submission_limit):
        if not submission.stickied: # TODO: stickied are filtered out

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

# connect to sql and write comments IDs to db TODO:
# new_comment_ids = list(set(new_comment_ids))
# new_comment_ids = [[item] for item in new_comment_ids]

# sql_header = 'comment_id'
# sql_insertion = f'INSERT INTO {config.sql_database}.dbo.{config.sql_table_comments} ({sql_header}) values(?)'

# if len(new_comment_ids) > 0:
#     with pyodbc.connect(config.sql_connection_string) as conn:
#         with conn.cursor() as cursor:
#             cursor.fast_executemany = True
#             cursor.executemany(sql_insertion, new_comment_ids)
#         conn.commit()
#         cursor.close()
#     print(f'successful: new comment IDs added to db {len(new_comment_ids)}')
# else:
#     print('no new comments')

# TODO: filter for ticker and count
all_ticker = []
with pyodbc.connect(config.sql_connection_string) as conn:
    with conn.cursor() as cursor:
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

df_ticker_found = pd.DataFrame(ticker_found, columns=['subreddit', 'crawl_date', 'ticker', 'amount'])
df_ticker_found = df_ticker_found.groupby(['subreddit', 'crawl_date', 'ticker']).agg({'amount': ['sum']})
df_ticker_found.reset_index(inplace=True)

print(df_ticker_found.head(15))

# TODO: update db