-- Week 6 challenge

-- Use Frosty Friday role created in week 1 
use role ff_role;

-- Use database for challenges created in week 1
use database frosty_friday;

-- Set warehouse
use warehouse compute_wh;

-- Create schema for week6 challenge
create schema week6;

-- Create stage for the s3 bucket
create stage ff_week6_stg
url = 's3://frostyfridaychallenges/challenge_6/';

-- List the contents of the stage
list @ff_week6_stg;
/* listed the 2 csv-files
s3://frostyfridaychallenges/challenge_6/nations_and_regions.csv
s3://frostyfridaychallenges/challenge_6/westminster_constituency_points.csv
*/

-- Create file format for the csv files
create or replace file format ff_csv_fileformat
type = csv
field_optionally_enclosed_by = '"'
skip_header = 1;

---------------------------------
-- Select the 6 columns from nations_and_regions.csv and create view
---------------------------------
create or replace view ff_nations_and_regions_v
as
select $1::string as nation_or_region_name
,      $2::string as type
,      $3::int as sequence_num
,      $4::float as longitude
,      $5::float as latitude
,      $6::int as part
from @ff_week6_stg/nations_and_regions.csv
(file_format => 'ff_csv_fileformat');

---------------------------------
-- Select the 5 columns from westminster_constituency_points.csv and create view
---------------------------------
create or replace view ff_westminster_constituency_points_v
as
select $1::string as constituency
,      $2::int as sequence_num
,      $3::float as longitude
,      $4::float as latitude
,      $5::int as part
from @ff_week6_stg/westminster_constituency_points.csv
(file_format => 'ff_csv_fileformat');

select *
from ff_nations_and_regions_v;

---------------------------------
-- Developing...
-- Make coordinate pairs and polygons to visualize in https://clydedacruz.github.io/openstreetmap-wkt-playground/
select nation_or_region_name
, part
, 'POLYGON(('||
  listagg(longitude || ' ' || latitude, ',') within group (order by sequence_num)
  ||'))' as polygon
from ff_nations_and_regions_v
group by nation_or_region_name, part;

select constituency
, part
, 'POLYGON(('||
  listagg(longitude || ' ' || latitude, ',') within group (order by sequence_num)
  ||'))' as polygon
from ff_westminster_constituency_points_v
group by constituency, part;

-- Make Snowflake Geography polygons at part-level
select nation_or_region_name
, part
, to_geography('POLYGON(('||
  listagg(longitude || ' ' || latitude, ',') within group (order by sequence_num)
  ||'))') as polygon
from ff_nations_and_regions_v
group by nation_or_region_name, part;

select constituency
, part
, to_geography('POLYGON(('||
  listagg(longitude || ' ' || latitude, ',') within group (order by sequence_num)
  ||'))') as polygon
from ff_westminster_constituency_points_v
group by constituency, part;

---------------------------------
-- Combine the above part-level polygons 
-- to nation/country and constituency levels
-- and count number of intersections
---------------------------------
with nations_and_regions_parts as
    (select nation_or_region_name
    , type
    , part
    , to_geography('POLYGON(('||
      listagg(longitude || ' ' || latitude, ',') within group (order by sequence_num)
      ||'))') as polygon
    from ff_nations_and_regions_v
    group by nation_or_region_name, type, part
    )
, westminster_constituency_points_parts as
    (select constituency
    , part
    , to_geography('POLYGON(('||
      listagg(longitude || ' ' || latitude, ',') within group (order by sequence_num)
      ||'))') as polygon
    from ff_westminster_constituency_points_v
    group by constituency, part
    )
, nations_and_regions as
    (select nation_or_region_name
    , st_collect(nrp.polygon) as polygon
    from nations_and_regions_parts nrp
    group by nation_or_region_name
    )
, westminster_constituency_points as
    (select constituency
    , st_collect(wcpp.polygon) as polygon
    from westminster_constituency_points_parts wcpp
    group by constituency
    )
, intersections as
    (select nr.nation_or_region_name
          , st_intersects(nr.polygon, wcp.polygon) intersects
     from nations_and_regions nr
     ,    westminster_constituency_points wcp
    )
select i.nation_or_region_name as nation_or_region
,      count(*) as intersecting_constituencies
from   intersections i
where  i.intersects = true
group by i.nation_or_region_name;

