-- Week 4 Challenge

-- Use Frosty Friday role 
use role ff_role;

-- Use database for challenges
use database frosty_friday;

-- Set warehouse
use warehouse compute_wh;

-- Create schema for week4 challenge
create schema week4;

-- Create stage for the S3 bucket
create stage ff_week4_stg
url = 's3://frostyfridaychallenges/challenge_4/';

-- List the contents of the stage
list @ff_week4_stg;
-- Listed s3://frostyfridaychallenges/challenge_4/Spanish_Monarchs.json

-- Create file format for the Json-file 
create or replace file format ff_json_file_format
  type = json
  strip_outer_array = true;

-- Query the file in S3
select $1
from @ff_week4_stg
(file_format => ff_json_file_format);

-- Create table to load the data into
create or replace table ff_spanish_monarchs_load
(json_data variant);

-- Load data into table
copy into ff_spanish_monarchs_load
  from @ff_week4_stg
  file_format = (format_name = ff_json_file_format);
  
-- Check the data
select *
from ff_spanish_monarchs_load;

-- Create the final table for parsed data
create or replace table ff_spanish_monarchs
( id number autoincrement
, inter_house_id number
, era varchar
, house varchar
, name varchar
, nickname_1 varchar
, nickname_2 varchar
, nickname_3 varchar
, birth date
, place_of_birth varchar
, start_of_reign date
, queen_or_queen_consort_1 varchar
, queen_or_queen_consort_2 varchar
, queen_or_queen_consort_3 varchar
, end_of_reign date
, duration varchar
, death date
, age_at_time_of_death_years number
, place_of_death varchar
, burial_place varchar
);

-- Insert data
insert into ff_spanish_monarchs
( inter_house_id
, era 
, house 
, name 
, nickname_1 
, nickname_2 
, nickname_3 
, birth 
, place_of_birth 
, start_of_reign 
, queen_or_queen_consort_1 
, queen_or_queen_consort_2 
, queen_or_queen_consort_3 
, end_of_reign 
, duration 
, death 
, age_at_time_of_death_years 
, place_of_death 
, burial_place 
)
select 
  mo.index + 1 as inter_house_id
, sm.json_data:"Era"::string as era
, ho.value:"House"::string as house
, mo.value:"Name"::string as name
, nvl(mo.value:"Nickname"[0], mo.value:"Nickname")::string as nickname_1
, mo.value:"Nickname"[1]::string as nickname_2
, mo.value:"Nickname"[2]::string as nickname_3
, mo.value:"Birth"::date as birth
, mo.value:"Place of Birth"::varchar as place_of_birth 
, mo.value:"Start of Reign"::date as start_of_reign 
, nvl(mo.value:"Consort\/Queen Consort"[0], mo.value:"Consort\/Queen Consort")::string as queen_or_queen_consort_1
, nvl(mo.value:"Consort\/Queen Consort"[1], mo.value:"Consort\/Queen Consort")::string as queen_or_queen_consort_2
, nvl(mo.value:"Consort\/Queen Consort"[2], mo.value:"Consort\/Queen Consort")::string as queen_or_queen_consort_3
, mo.value:"End of Reign"::date as end_of_reign 
, mo.value:"Duration"::string as duration 
, mo.value:"Death"::date as death 
, split_part(mo.value:"Age at Time of Death",' ', 1)::number as age_at_time_of_death_years 
, mo.value:"Place of Death"::string as place_of_death 
, mo.value:"Burial Place"::string as burial_place 
from ff_spanish_monarchs_load sm
, lateral flatten(input => sm.json_data:Houses) ho
, lateral flatten(input => ho.value:Monarchs) mo
order by birth
, inter_house_id;

-- Result
select *
from ff_spanish_monarchs
order by id;
