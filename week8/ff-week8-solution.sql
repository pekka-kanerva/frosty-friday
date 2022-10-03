-- Week 8 Challenge

-- Use Frosty Friday role 
use role ff_role;

-- Use database for challenges
use database frosty_friday;

-- Set warehouse
use warehouse compute_wh;

-- Create schema for week8 challenge
create schema week8;

-- Create stage for the S3 bucket
create stage ff_week8_stg
url = 's3://frostyfridaychallenges/challenge_8/';

-- List the contents of the stage
list @ff_week8_stg;
-- Listed s3://frostyfridaychallenges/challenge_8/payments.csv

-- Create file format for the csv file
create or replace file format ff_csv_fileformat
type = csv
skip_header = 1;

-- Query the file in S3
select $1, $2, $3, $4
from @ff_week8_stg
(file_format => ff_csv_fileformat);

-- Create table to load the data into
create or replace table ff_payments_tbl
( id int
, payment_date timestamp
, card_type string
, amount_spent float
);

-- Load data into table
copy into ff_payments_tbl
  from @ff_week8_stg
  file_format = (format_name = ff_csv_fileformat);
  
-- Check the data
select *
from ff_payments_tbl;

----------------------------
-- Developing queries...
----------------------------

select min(payment_date) 
     , max(payment_date) 
from ff_payments_tbl;

select payment_date, trunc(payment_date, 'WEEK') 
from ff_payments_tbl
limit 10;

-- Final query to be used in Streamlit
select trunc(payment_date, 'WEEK') as payment_date
     , sum(amount_spent) as amount_spent
from ff_payments_tbl
group by trunc(payment_date, 'WEEK')
order by 1;
