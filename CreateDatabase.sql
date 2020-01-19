CREATE DATABASE IF NOT EXISTS etl_hd;
USE etl_hd;
CREATE TABLE IF NOT EXISTS dim_time (dt TIMESTAMP, day INT, month INT, year INT) STORED AS ORC;
CREATE TABLE IF NOT EXISTS dim_location (country_code STRING, country STRING, city STRING, zipcode STRING, location_id INT) STORED AS ORC;
CREATE TABLE IF NOT EXISTS dim_price (price_range STRING, min_price DOUBLE, max_price DOUBLE, price_id INT) STORED AS ORC;
CREATE TABLE IF NOT EXISTS dim_location_score (location_score_id INT, min_location_score INT, max_location_score INT, location_score_range STRING) STORED AS ORC;
CREATE TABLE IF NOT EXISTS f_fact (dt TIMESTAMP, location_id INT, location_score_id INT, price_id INT, sum_review_scores_value FLOAT, sum_price FLOAT, available_count INT, not_available_count INT, bedrooms INT, bathrooms INT) STORED AS ORC;
