USE ROLE accountadmin;

-- Create Frosty Friday challenges -role and give grants
CREATE ROLE ff_role;
GRANT CREATE DATABASE ON account TO ROLE ff_role; 
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE ff_role;
GRANT ROLE ff_role TO ROLE sysadmin;

--
-- Use Frosty Friday role 
USE ROLE ff_role;

-- Create database for challenges
CREATE DATABASE frosty_friday;

-- Create schema for week1 challenge
CREATE SCHEMA week1;

-- Create stage for the S3 bucket
CREATE STAGE ff_week1_stg
url = 's3://frostyfridaychallenges/challenge_1/';

-- List the contents of the stage
LIST @ff_week1_stg;
-- Listed 3 csv-files

-- Select some columns from the csv-files in the stage to check what's in the files
SELECT metadata$filename
,      metadata$file_row_number
,      $1
,      $2
,      $3
FROM @ff_week1_stg
ORDER BY 1, 2;

-- There is only 1 column.
-- Each file has header-row "result" -> skip it

-- File format for the csv files
CREATE OR REPLACE FILE FORMAT ff_csv_fileformat
TYPE = csv
SKIP_HEADER = 1;

-- Create table for the data
CREATE OR REPLACE TABLE ff_week1_tbl 
  ( filename VARCHAR
  , row_number NUMBER
  , col1 VARCHAR);

-- Load data to table
COPY INTO ff_week1_tbl
FROM (
    SELECT metadata$filename
    ,      metadata$file_row_number
    ,      $1
    FROM @ff_week1_stg
    (FILE_FORMAT => 'ff_csv_fileformat')
    );

-- Check results
SELECT * FROM ff_week1_tbl order by 1, 2;

