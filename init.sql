-- Create airflow DB if not exists
CREATE DATABASE airflow;

-- Switch to stockdata and create schema
\c stockdata;

-- Create the stock_data table
CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(30) NOT NULL,
    date_recorded TIMESTAMP NOT NULL, -- <-- keep full datetime
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date_recorded) -- now uniqueness is per symbol+datetime
);


-- Create an index for better query performance
CREATE INDEX IF NOT EXISTS idx_stock_symbol_date 
    ON stock_data(symbol, date_recorded);
