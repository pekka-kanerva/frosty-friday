-- Week 2 Challenge

-- Use Frosty Friday role created in Week 1
USE ROLE ff_role;

-- Use database for challenges created in Week 1
USE DATABASE frosty_friday;

-- Set warehouse
USE WAREHOUSE compute_wh;

-- Create schema for week2 challenge
CREATE SCHEMA week2;

-- Create internal stage to put the Parque-file into
CREATE STAGE ff_week2_stg;

-- Create file format for the Parquet-file 
CREATE FILE FORMAT ff_parquet_file_format
  TYPE = PARQUET;


-- Put Parque-file into ff_week2_stg with snowsql
-- snowsql -a <my account> 
-- User: <my_username>
-- Password:
--
-- use database FROSTY_FRIDAY;
-- use schema WEEK2;
-- put file://c:\temp\employees.parquet @ff_week2_stg;

-- Query the file
SELECT $1
FROM @ff_week2_stg/employees.parquet
(FILE_FORMAT => ff_parquet_file_format);

-- Query columns in the file
select *
  from table(
    infer_schema(
      location=>'@ff_week2_stg/employees.parquet'
      , file_format=>'ff_parquet_file_format'
      )
    );

-- Create table to load the data into
create or replace table ff_employees
  using template (
    select array_agg(object_construct(*))
      from table(
        infer_schema(
          location=>'@ff_week2_stg/employees.parquet',
          file_format=>'ff_parquet_file_format'
        )
      ));

-- Load data
COPY INTO ff_employees
FROM 
( select $1:email::TEXT
  , $1:country::TEXT
  , $1:country_code::TEXT
  , $1:education::TEXT
  , $1:postcode::TEXT
  , $1:first_name::TEXT
  , $1:street_name::TEXT
  , $1:job_title::TEXT
  , $1:city::TEXT
  , $1:employee_id::NUMBER(38, 0)
  , $1:last_name::TEXT
  , $1:time_zone::TEXT
  , $1:street_num::NUMBER(38, 0)
  , $1:payroll_iban::TEXT
  , $1:suffix::TEXT
  , $1:dept::TEXT
  , $1:title::TEXT 
  from @ff_week2_stg/employees.parquet
  (file_format => ff_parquet_file_format)
);

-- Check data from table
SELECT *
FROM ff_employees;

-- Create view in order to track changes only to DEPT and JOB_TITLE columns
create or replace view ff_emp_v as select "employee_id", "dept", "job_title" from ff_employees;

-- Create stream on the view
create stream ff_emp_v_stream on view ff_emp_v;

-- Issue the given updates
UPDATE ff_employees SET "country" = 'Japan' WHERE "employee_id" = 8;
UPDATE ff_employees SET "last_name" = 'Forester' WHERE "employee_id" = 22;
UPDATE ff_employees SET "dept" = 'Marketing' WHERE "employee_id" = 25;
UPDATE ff_employees SET "title" = 'Ms' WHERE "employee_id" = 32;
UPDATE ff_employees SET "job_title" = 'Senior Financial Analyst' WHERE "employee_id" = 68;

-- Query the stream
select * 
from ff_emp_v_stream;
