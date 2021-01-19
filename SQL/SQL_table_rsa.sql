DROP TABLE IF EXISTS dbo.rsa_table;
CREATE TABLE rsa_table (
    subreddit varchar(255),
    crawl_date DATE,
    ticker varchar(255),
    amount INT
);