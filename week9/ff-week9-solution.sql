-- Week 9 challenge

-- Use Frosty Friday role created in week 1 
use role ff_role;

-- Use database for challenges created in week 1
use database frosty_friday;

-- Set warehouse
use warehouse compute_wh;

-- Create schema for week9 challenge
create schema week9;

----------------------------------------
-- Setup steps as given in the challenge
----------------------------------------
-- CREATE DATA 

CREATE OR REPLACE TABLE data_to_be_masked(first_name varchar, last_name varchar,hero_name varchar);
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Eveleen', 'Danzelman','The Quiet Antman');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Harlie', 'Filipowicz','The Yellow Vulture');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Mozes', 'McWhin','The Broken Shaman');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Horatio', 'Hamshere','The Quiet Charmer');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Julianna', 'Pellington','Professor Ancient Spectacle');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Grenville', 'Southouse','Fire Wonder');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Analise', 'Beards','Purple Fighter');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Darnell', 'Bims','Mister Majestic Mothman');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Micky', 'Shillan','Switcher');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Ware', 'Ledstone','Optimo');

--CREATE ROLES and give grants
use role accountadmin;
CREATE ROLE foo1;
CREATE ROLE foo2;
GRANT ROLE foo1 TO USER <my username>;
GRANT ROLE foo2 TO USER <my username>;

grant usage on database frosty_friday to role foo1;
grant usage on schema frosty_friday.week9 to role foo1;
grant usage on warehouse compute_wh to role foo1;
grant select on frosty_friday.week9.data_to_be_masked to role foo1;

grant usage on database frosty_friday to role foo2;
grant usage on schema frosty_friday.week9 to role foo2;
grant usage on warehouse compute_wh to role foo2;
grant select on frosty_friday.week9.data_to_be_masked to role foo2;

----------------------------------------
-- Create tag and masking policy
----------------------------------------

create or replace tag hero_name_security_level allowed_values 'low', 'high'; -- low sees only hero_name, high sees hero_name and first_name

create or replace masking policy hero_names_mask as (val string) returns string ->
  case
    when (is_role_in_session('FOO1') or is_role_in_session('FOO2')) and system$get_tag_on_current_column('hero_name_security_level') = 'low' then val 
    when is_role_in_session('FOO2') and system$get_tag_on_current_column('hero_name_security_level') = 'high' then val 
    else '***MASKED***'
  end;

alter tag hero_name_security_level set masking policy hero_names_mask;

----------------------------------------
-- Apply tag to table
----------------------------------------
alter table frosty_friday.week9.data_to_be_masked modify column 
   first_name set tag hero_name_security_level = 'low'
 , last_name  set tag hero_name_security_level = 'high';

----------------------------------------
-- Test the given roles and selects
----------------------------------------
use role accountadmin;

select *
from data_to_be_masked;

use role foo1;

select *
from data_to_be_masked;

use role foo2;

select *
from data_to_be_masked;
