-- Week 3 challenge

-- Use Frosty Friday role created in week 1 
use role ff_role;

-- Use database for challenges created in week 1
use database frosty_friday;

-- Set warehouse
use warehouse compute_wh;

-- Create schema for week3 challenge
create schema week3;

-- Create stage for the s3 bucket
create stage ff_week3_stg
url = 's3://frostyfridaychallenges/challenge_3/';

-- List the contents of the stage
list @ff_week3_stg;
/* listed 9 csv-files
s3://frostyfridaychallenges/challenge_3/keywords.csv
s3://frostyfridaychallenges/challenge_3/week3_data1.csv
s3://frostyfridaychallenges/challenge_3/week3_data2.csv
s3://frostyfridaychallenges/challenge_3/week3_data2_stacy_forgot_to_upload.csv
s3://frostyfridaychallenges/challenge_3/week3_data3.csv
s3://frostyfridaychallenges/challenge_3/week3_data4.csv
s3://frostyfridaychallenges/challenge_3/week3_data4_extra.csv
s3://frostyfridaychallenges/challenge_3/week3_data5.csv
s3://frostyfridaychallenges/challenge_3/week3_data5_added.csv
*/

-- Select some columns from the csv-files in the stage to check what's in the files
select metadata$filename
,      metadata$file_row_number
,      $1
,      $2
,      $3
,      $4
,      $5
,      $6
,      $7
from @ff_week3_stg
order by 1, 2;

-- keywords.csv seems to have 3 columns and a header row
-- Data files seems to have 5 columns and a header row

-- Create file format for the csv files
create or replace file format ff_csv_fileformat
type = csv
skip_header = 1;

--------------------------------------
-- Load data in datafiles
--------------------------------------
-- Create table for the data
create table ff_week3_data
(id number
,first_name varchar
,last_name varchar
,catch_phrase varchar
,timestamp date
);

-- Load data
copy into ff_week3_data
from @ff_week3_stg
file_format = (format_name = ff_csv_fileformat)
pattern = 'challenge_3/week3_data.*[.]csv';

--------------------------------------
-- Building the insert statement for files to be tracked
--------------------------------------
-- 1. query to get number of rows in the files
select metadata$filename as filename
, count(*) as number_of_rows
from @ff_week3_stg
(file_format => 'ff_csv_fileformat')
group by metadata$filename;

-- 2. query to get the keywords from the keywords-file
select $1 as filename_pattern
from @ff_week3_stg/keywords.csv
(file_format => 'ff_csv_fileformat');

--------------------------------------
-- create table for results
--------------------------------------
create table ff_files_to_track
(filename varchar
,number_of_rows number);

--------------------------------------
-- insert filenames to be tracked into the table
--------------------------------------
insert into ff_files_to_track
-- cte to get number of rows in the files
with all_files as
(select metadata$filename as filename
 , count(*) as number_of_rows
 from @ff_week3_stg 
   (file_format => 'ff_csv_fileformat')
 group by metadata$filename
)
select filename
,      number_of_rows
from all_files a
-- get only files whose name matches the pattern in the keywords-file
where exists ( 
    select 1 
    from @ff_week3_stg/keywords.csv
      (file_format => 'ff_csv_fileformat')
    where a.filename like '%' || $1 || '%'
    )
;

select *
from ff_files_to_track;
