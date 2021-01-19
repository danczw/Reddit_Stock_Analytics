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
submission_limit = 3 # set number of submissions crawled per subreddit

# TODO: get existing comment ids already in db
comments_data = []
with pyodbc.connect(config.sql_connection_string) as conn:
    with conn.cursor() as cursor:
        cursor.execute('SELECT * FROM rsa_table')
        for row in cursor.fetchall():
            comments_data.append(row)

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
df = pd.DataFrame(columns=col_header)

for s in subs:
    for submission in reddit.subreddit(s).hot(limit=submission_limit):
        if not submission.stickied: # TODO: stickied are filtered out

            submission.comments.replace_more(limit=None) # get CommentForest
            for comment in submission.comments.list():
                
                # TODO: check if comment id already in table
                if comment.id in comments_data:
                    pass
                else:
                    # initialize list and append meta data + body
                    comments_list = []
                    comments_list.append(s)
                    comments_list.append(comment.id)
                    comments_list.append(
                        datetime.utcfromtimestamp(comment.created_utc).date()
                    )
                    comments_list.append(
                        comment.body.encode('ascii', 'ignore').lower()
                    )

                    # save comment to df
                    temp_df = pd.DataFrame([comments_list], columns=col_header)
                    df = df.append(temp_df)

# connect to sql and write comments to db
sql_header = 'comment_id'
sql_insertion = f'INSERT INTO {config.sql_database}.dbo.{config.sql_table_comments} ({sql_header}) values(?)'

with pyodbc.connect(config.sql_connection_string) as conn:
    with conn.cursor() as cursor:
        for index, row in df.iterrows():
            cursor.execute(
                sql_insertion,
                row.comment_id,
            )

    conn.commit()
    cursor.close()

# TODO: filter for ticker and count
# TODO: update db