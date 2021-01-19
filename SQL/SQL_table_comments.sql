DROP TABLE IF EXISTS dbo.comment_table;
CREATE TABLE comment_table (
    subreddit varchar(255),
    comment_id varchar(255),
    created_utc DATE,
    body TEXT
);