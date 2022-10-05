-- Week 10 challenge

-- Use Frosty Friday role created in week 1 
use role ff_role;

-- Use database for challenges created in week 1
use database frosty_friday;

-- Set warehouse
use warehouse compute_wh;

-- Create schema for week10 challenge
create schema week10;

----------------------------------------
-- Setup steps as given in the challenge
----------------------------------------
use role accountadmin;

-- Create the warehouses
create warehouse if not exists my_xsmall_wh 
    with warehouse_size = XSMALL
    auto_suspend = 120;

grant usage on warehouse my_xsmall_wh to role ff_role;

create warehouse if not exists my_small_wh 
    with warehouse_size = SMALL
    auto_suspend = 120;

grant usage on warehouse my_small_wh to role ff_role;

-- Create the table
use role ff_role;
use schema week10;
use warehouse compute_wh;

create or replace table ff_week10_tbl
(
    date_time datetime,
    trans_amount double
);

-- Create the stage
create or replace stage week_10_frosty_friday_stage
    url = 's3://frostyfridaychallenges/challenge_10/';
	
list @week_10_frosty_friday_stage;
/* Listed:
name	                                                size
s3://frostyfridaychallenges/challenge_10/2022-07-01.csv	5,002
s3://frostyfridaychallenges/challenge_10/2022-07-02.csv	14,963
s3://frostyfridaychallenges/challenge_10/2022-07-03.csv	9,987
s3://frostyfridaychallenges/challenge_10/2022-07-04.csv	22,429
s3://frostyfridaychallenges/challenge_10/2022-07-05.csv	14,974
s3://frostyfridaychallenges/challenge_10/2022-07-06.csv	7,489
s3://frostyfridaychallenges/challenge_10/2022-07-07.csv	24,925
*/

-- Check the contents of the files
create or replace file format ff_check_files
    type = CSV
    field_delimiter = NONE
    record_delimiter = NONE
    skip_header = 0;

select 
    metadata$filename::string as file_name
  , metadata$file_row_number as row_number
  , $1::variant as contents
from @week_10_frosty_friday_stage
  (file_format => 'ff_check_files')
order by 
    file_name
  , row_number
;

-- 2 columns, comma-limited, new line \n, 1 header row

-- Create the correct file format
create or replace file format ff_csv_file_format
    type = CSV
    field_delimiter = ','
    record_delimiter = '\n'
    skip_header = 1
;

-- recreate stage with correct file format	
create or replace stage week_10_frosty_friday_stage
    url = 's3://frostyfridaychallenges/challenge_10/'
    file_format = ff_csv_file_format;
    
-- Test by querying files    
select 
    metadata$filename::string as file_name
  , metadata$file_row_number as row_number
  , $1
  , $2
from @week_10_frosty_friday_stage
order by 
    file_name
  , row_number;

---------------------------------
-- Create the stored procedure 
---------------------------------
create or replace procedure dynamic_warehouse_data_load(stage_name string, table_name string)
returns varchar
language sql
execute as caller
as
  declare
    -- Constants
    C_XSMALL_WH varchar default 'my_xsmall_wh';
    C_SMALL_WH  varchar default 'my_small_wh';
    C_FILE_SIZE_LIMIT number default 10000;
    
    -- Variables
    v_list_command varchar default 'list @' || stage_name;
    res_files resultset;
    v_warehouse_name varchar;
    v_use_wh_command varchar;
    v_file_name varchar;
    v_copy_command varchar;
    res_copy resultset;
    n_total_rows_loaded number default 0;
    
  begin
    -- Get list of files into resultset
    res_files := (execute immediate v_list_command);
    
    -- Define cursor for the resultset
    let c_files cursor for res_files;
    
    -- Loop the files
    for rec_file in c_files do

      -- Get the warehouse name to be used
      if ( rec_file."size" >= C_FILE_SIZE_LIMIT ) then
        v_warehouse_name := C_SMALL_WH;
      else
        v_warehouse_name := C_XSMALL_WH;
      end if;
      
      -- Set the warehouse
      v_use_wh_command := 'use warehouse ' || v_warehouse_name;
      execute immediate v_use_wh_command;
      
      -- Get the file name part from the result of list-command, eg. s3://frostyfridaychallenges/challenge_10/2022-07-01.csv
      v_file_name := split_part(rec_file."name", '/', 5);
      
      -- Load the data
      v_copy_command := 'copy into ' || table_name || ' from @' || stage_name || '/' || v_file_name;
      res_copy := (execute immediate v_copy_command);
      
      -- Define cursor for the resultset
      let c_res_copy cursor for res_copy;
    
      -- Loop the ONE result row and add to total rows loaded -counter.
      for rec_res_copy in c_res_copy do
        n_total_rows_loaded := n_total_rows_loaded + rec_res_copy."rows_loaded";
      end for;
      
    end for;
    
    return n_total_rows_loaded || ' rows were added';

  end;


-- Call the stored procedure.
call dynamic_warehouse_data_load('week_10_frosty_friday_stage', 'ff_week10_tbl');

-- Output:
-- DYNAMIC_WAREHOUSE_DATA_LOAD
-- 4000 rows were added

/* Query history:
QUERY_ID	QUERY_TEXT	DATABASE_NAME	SCHEMA_NAME	QUERY_TYPE
01a76c1d-3201-559e-0000-9a3900566616	select * from table(information_schema.query_history_by_session()) order by start_time desc;	FROSTY_FRIDAY	WEEK10	UNKNOWN
01a76c16-3201-5652-0000-9a3900567116	copy into ff_week10_tbl from @week_10_frosty_friday_stage/2022-07-07.csv	FROSTY_FRIDAY	WEEK10	COPY
01a76c16-3201-5652-0000-9a3900567112	use warehouse my_small_wh	FROSTY_FRIDAY	WEEK10	USE
01a76c16-3201-5652-0000-9a390056710e	copy into ff_week10_tbl from @week_10_frosty_friday_stage/2022-07-06.csv	FROSTY_FRIDAY	WEEK10	COPY
01a76c16-3201-5652-0000-9a390056710a	use warehouse my_xsmall_wh	FROSTY_FRIDAY	WEEK10	USE
01a76c16-3201-5652-0000-9a3900567106	copy into ff_week10_tbl from @week_10_frosty_friday_stage/2022-07-05.csv	FROSTY_FRIDAY	WEEK10	COPY
01a76c16-3201-5652-0000-9a3900567102	use warehouse my_small_wh	FROSTY_FRIDAY	WEEK10	USE
01a76c16-3201-5652-0000-9a39005670fe	copy into ff_week10_tbl from @week_10_frosty_friday_stage/2022-07-04.csv	FROSTY_FRIDAY	WEEK10	COPY
01a76c16-3201-5652-0000-9a39005670fa	use warehouse my_small_wh	FROSTY_FRIDAY	WEEK10	USE
01a76c16-3201-5652-0000-9a39005670f6	copy into ff_week10_tbl from @week_10_frosty_friday_stage/2022-07-03.csv	FROSTY_FRIDAY	WEEK10	COPY
01a76c16-3201-5652-0000-9a39005670f2	use warehouse my_xsmall_wh	FROSTY_FRIDAY	WEEK10	USE
01a76c16-3201-5652-0000-9a39005670ee	copy into ff_week10_tbl from @week_10_frosty_friday_stage/2022-07-02.csv	FROSTY_FRIDAY	WEEK10	COPY
01a76c16-3201-5652-0000-9a39005670ea	use warehouse my_small_wh	FROSTY_FRIDAY	WEEK10	USE
01a76c16-3201-5652-0000-9a39005670e6	copy into ff_week10_tbl from @week_10_frosty_friday_stage/2022-07-01.csv	FROSTY_FRIDAY	WEEK10	COPY
01a76c16-3201-5652-0000-9a39005670e2	use warehouse my_xsmall_wh	FROSTY_FRIDAY	WEEK10	USE
01a76c16-3201-5652-0000-9a39005670de	list @week_10_frosty_friday_stage	FROSTY_FRIDAY	WEEK10	LIST_FILES
01a76c16-3201-5652-0000-9a39005670da	call dynamic_warehouse_data_load('week_10_frosty_friday_stage', 'ff_week10_tbl');	FROSTY_FRIDAY	WEEK10	SELECT
*/
