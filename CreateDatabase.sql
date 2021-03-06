CREATE DATABASE IF NOT EXISTS etl_hd;
USE etl_hd;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_price;
DROP TABLE IF EXISTS dim_location_score;
DROP TABLE IF EXISTS f_fact;
CREATE TABLE IF NOT EXISTS dim_time (dt TIMESTAMP, day INT, month INT, year INT) STORED AS ORC;
CREATE TABLE IF NOT EXISTS dim_location (country_code STRING, country STRING, city STRING, zipcode STRING, location_id STRING) STORED AS ORC;
CREATE TABLE IF NOT EXISTS dim_price (price_id INT, min_price DOUBLE, max_price DOUBLE, price_range STRING) STORED AS ORC;
CREATE TABLE IF NOT EXISTS dim_location_score (location_score_id INT, min_location_score INT, max_location_score INT, location_score_range STRING) STORED AS ORC;
CREATE TABLE IF NOT EXISTS f_fact (dt TIMESTAMP, location_id STRING, price_id INT, location_score_id INT, bathrooms DOUBLE, bedrooms INT, sum_price DOUBLE, sum_review_score FLOAT, available_count INT, not_available_count INT) STORED AS ORC;
