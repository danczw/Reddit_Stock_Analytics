DROP TABLE IF EXISTS dbo.ticker_table;
CREATE TABLE ticker_table (
    comp_ticker varchar(255),
    comp_name varchar(255),
    exchange varchar(255),
    country varchar(255),
);