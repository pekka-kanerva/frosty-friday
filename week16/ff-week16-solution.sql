-- Week 16 challenge

-- Use Frosty Friday role created in week 1 
use role ff_role;

-- Use database for challenges created in week 1
use database frosty_friday;

-- Set warehouse
use warehouse compute_wh;

-- Create schema for week16 challenge
create schema week16;

-- Create file format, schema and table as given in challenge
create or replace file format json_ff
    type = json
    strip_outer_array = TRUE;
    
create or replace stage week_16_frosty_stage
    url = 's3://frostyfridaychallenges/challenge_16/'
    file_format = json_ff;

create or replace table week16.week16 as
select t.$1:word::text word, t.$1:url::text url, t.$1:definition::variant definition  
from @week_16_frosty_stage (file_format => 'json_ff', pattern=>'.*week16.*') t;

-- solution
select *
from (
    select w.word
        ,  w.url
        ,  m.value:partOfSpeech::string as part_of_speech
        ,  m.value:synonyms::variant as general_synonyms
        ,  m.value:antonyms::variant as general_antonyms
        ,  def.value:definition::string as definition
        ,  def.value:example::string as example_if_applicable
        ,  def.value:synonyms::variant as definitional_synonyms
        ,  def.value:antonyms::variant as definitional_antonyms
    from week16 w
    , lateral flatten(input => w.definition, outer => TRUE, mode => 'ARRAY') d
    , lateral flatten(input => d.value:meanings, outer => TRUE, mode => 'ARRAY') m
    , lateral flatten(input => m.value:definitions, outer => TRUE, mode => 'ARRAY') def 
) sub
where word like 'l%'
;

-- counts
select count(word)
 ,     count(distinct word)
from (
    select w.word
        ,  w.url
        ,  m.value:partOfSpeech::string as part_of_speech
        ,  m.value:synonyms::variant as general_synonyms
        ,  m.value:antonyms::variant as general_antonyms
        ,  def.value:definition::string as definition
        ,  def.value:example::string as example_if_applicable
        ,  def.value:synonyms::variant as definitional_synonyms
        ,  def.value:antonyms::variant as definitional_antonyms
    from week16 w
    , lateral flatten(input => w.definition, outer => TRUE, mode => 'ARRAY') d
    , lateral flatten(input => d.value:meanings, outer => TRUE, mode => 'ARRAY') m
    , lateral flatten(input => m.value:definitions, outer => TRUE, mode => 'ARRAY') def
) sub;
