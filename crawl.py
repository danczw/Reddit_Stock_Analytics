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
    'crawl_datetime', 'subreddit', 'submission', 'submission_id', 'stickied',
    'comment_id', 'created_utc', 'body', 'score'
]
df = pd.DataFrame(columns=col_header)

for s in subs:
    for submission in reddit.subreddit(s).hot(limit=submission_limit):
        if not submission.stickied: # TODO: stickied are currently filtered out

            submission.comments.replace_more(limit=None) # get whole CommentForest
            for comment in submission.comments.list():
                # initialize list for current comment and append meta data + body
                comments_list = []
                comments_list.append(current_Time)
                comments_list.append(s)
                comments_list.append(submission.title.lower()) # TODO: fix encoding
                comments_list.append(submission.id)
                comments_list.append(submission.stickied)
                comments_list.append(comment.id)
                comments_list.append(comment.created_utc)
                comments_list.append(comment.body.lower()) # TODO: fix encoding
                comments_list.append(comment.score)

                # save comment to df
                temp_df = pd.DataFrame([comments_list], columns=col_header)
                df = df.append(temp_df)

# # save dfs
# rsa_subm_df.to_csv('submissions.csv', index=False)
# df.to_csv('comments.csv', index=False)

# connect to sql and write to db
sql_header = 'crawl_datetime, subreddit, submission, submission_id, stickied, comment_id, created_utc, body, score'
sql_insertion = f'INSERT INTO {config.sql_database}.dbo.{config.sql_table_comments} ({sql_header}) values(?,?,?,?,?,?,?,?,?)'

with pyodbc.connect(config.sql_connection_string) as conn:
    with conn.cursor() as cursor:
        for index, row in df.iterrows():
            cursor.execute(
                sql_insertion,
                row.crawl_datetime,
                row.subreddit,
                row.submission,
                row.submission_id,
                row.stickied,
                row.comment_id,
                row.created_utc,
                row.body,
                row.score
            )

    conn.commit()
    cursor.close()
