-- Week 5 Challenge

-- Use Frosty Friday role 
use role ff_role;

-- Use database for challenges
use database frosty_friday;

-- Set warehouse
use warehouse compute_wh;

-- Create schema for week5 challenge
create schema week5;

-- Create table to hold the sample data
create or replace table ff_week_5
(start_int number);

-- Generate data into table
insert into ff_week_5
select uniform(1, 10, random()) 
from table(generator(ROWCOUNT => 10));
  
-- Check the data
select *
from ff_week_5;

-- Create the Python UDF
create or replace function timesthree(p_start_int int)
returns int
language python
runtime_version = '3.8'
handler = 'timesthree'
as
$$
def timesthree(p_start_int):
  return p_start_int * 3
$$;

-- Result
select timesthree(start_int)
from FF_week_5;

