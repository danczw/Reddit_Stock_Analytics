'''
Python script to crawl submissions and all their comments from specified
subreddits using reddits praw API
'''

from azure.storage.blob import BlobServiceClient
import config
from datetime import datetime
from datetime import date
import pandas as pd
import praw

# config vars
current_Time = datetime.now()
current_date = date.today().strftime(r"%Y%m%d")
submission_limit = 2 # set number of submissions crawled per subreddit

# set reddit instance
reddit = praw.Reddit(
    client_id = config.client_id
    , client_secret = config.client_secret
    , user_agent = config.user_agent)
# define subreddits to be crawled
subs = ['wallstreetbets'] # , 'stocks', 'Finanzen', 'mauerstrassenwetten'


# define dataframe for submissions meta data # submission title
subm_col_header = ['crawl_datetime', 'sub', 'submission', 'submission_id',
                    'stickied', 'num_comments']
rsa_subm_df = pd.DataFrame(columns=subm_col_header)

# define dataframe for comments meta data + comment body
comments_col_header = ['crawl_datetime', 'submission_id', 'comment_id',
                    'created_utc', 'body', 'score']
rsa_comments_df = pd.DataFrame(columns=comments_col_header)

for s in subs:
    for submission in reddit.subreddit(s).hot(limit=submission_limit):
        if not submission.stickied: # TODO: stickied are currently filtered out
            # initialize list for current submission and append meta data
            subm_list = []
            subm_list.append(current_Time)
            subm_list.append(s)
            # TODO: fix encoding
            subm_list.append(submission.title.lower())
            subm_list.append(submission.id)
            subm_list.append(submission.stickied)
            subm_list.append(submission.num_comments)
            # save submission meta data to df
            temp_df = pd.DataFrame([subm_list], columns=subm_col_header)
            rsa_subm_df = rsa_subm_df.append(temp_df)
            
            submission.comments.replace_more(limit=None) # get whole CommentForest
            for comment in submission.comments.list():
                # initialize list for current comment and append meta data + body
                comments_list = []
                comments_list.append(current_Time)
                comments_list.append(submission.id)
                comments_list.append(comment.id)
                comments_list.append(comment.created_utc)
                # TODO: fix encoding
                comments_list.append(comment.body.lower())
                comments_list.append(comment.score)
                # save comment to df
                temp_df = pd.DataFrame([comments_list], columns=comments_col_header)
                rsa_comments_df = rsa_comments_df.append(temp_df)

# save dfs
rsa_subm_df.to_csv('submissions.csv', index=False)
rsa_comments_df.to_csv('comments.csv', index=False)

# azure blob config var
blob_service_client = BlobServiceClient.from_connection_string(config.storage_string)
blob_client = blob_service_client.get_blob_client(container=config.container_name, blob='submissions.csv')

# upload to blob
with open('submissions.csv', "rb") as data:
    blob_client.upload_blob(data)
