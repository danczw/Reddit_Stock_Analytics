DROP TABLE IF EXISTS dbo.rsa_table;
CREATE TABLE rsa_table (
    crawl_datetime DATETIME,
    subreddit varchar(255),
    submission varchar(255),
    submission_id varchar(255),
    stickied varchar(255),
    comment_id varchar(255),
    created_utc FLOAT,
    body TEXT,
    score INT
);