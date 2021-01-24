# reddit_stock_analytics
 
Python script to crawl submissions and all their comments from specified
subreddits using reddits praw API. Goal is to analyze stock ticker reddit talks about more than usual.

* reddit forums are crawled periodically using Azure web jobs
* comments are screened for ticker mentions (US and Deutsche Börse listed ticker)
* time series is created
