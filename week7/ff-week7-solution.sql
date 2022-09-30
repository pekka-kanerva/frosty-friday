use role accountadmin;
use database snowflake;
use schema account_usage;

-----------------------------
-- Developing...
-- Investigate account usage views that are propably needed in final query
-----------------------------
select *
from tags
limit 10;

select *
from tag_references
limit 10;

select *
from access_history
limit 10;

select *
from query_history
where role_name = 'USER1';

-----------------------------
-- Developing CTEs for the final query
-----------------------------

-- Tables with tag SECURITY_CLASS value Level Super Secret A+++++++
select object_database
     , object_schema
     , object_name
from tag_references
where tag_name = 'SECURITY_CLASS'
and tag_value = 'Level Super Secret A+++++++'
limit 10;

-- Access history
select query_id
, user_name
, f1.value:"objectName"::string as object_full_name
, split_part(object_full_name,'.',1) as db_name
, split_part(object_full_name,'.',2) as schema_name
, split_part(object_full_name,'.',3) as table_name
from access_history
     , lateral flatten(base_objects_accessed) f1
where f1.value:"objectName"::string = 'FF_WEEK_7.SUPER_VILLAINS.VILLAIN_INFORMATION'
and f1.value:"objectDomain"::string = 'Table'
and query_start_time >= dateadd('day', -1, current_timestamp())
limit 10;

-----------------------------
-- Final query
-----------------------------
with tagged_tables as
    ( select tag_name
           , tag_value
           , object_database
           , object_schema
           , object_name
      from tag_references
      where tag_name = 'SECURITY_CLASS'
      and tag_value = 'Level Super Secret A+++++++'
    )
, access_hist as
    ( select query_id
           , user_name
           , f1.value:"objectName"::string as object_full_name
           , split_part(object_full_name,'.',1) as db_name
           , split_part(object_full_name,'.',2) as schema_name
           , split_part(object_full_name,'.',3) as object_name
      from access_history
         , lateral flatten(base_objects_accessed) f1
      where f1.value:"objectName"::string like 'FF_WEEK_7%'
      and f1.value:"objectDomain"::string = 'Table'
      and query_start_time >= dateadd('day', -1, current_timestamp())
    )
, query_hist as
    ( select query_id
           , user_name
           , role_name
      from query_history
      where start_time >= dateadd('day', -1, current_timestamp())
    )
select t.tag_name
     , t.tag_value
     , min(q.query_id)
     , a.object_full_name
     , q.role_name
from tagged_tables t
inner join access_hist a on a.object_name = t.object_name
inner join query_hist q on q.query_id = a.query_id
group by t.tag_name
     , t.tag_value
     , a.object_full_name
     , q.role_name;

