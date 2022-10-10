-- Week 12 challenge

-- Use Frosty Friday role created in week 1 
use role ff_role;

-- Use database for challenges created in week 1
use database frosty_friday;

-- Set warehouse
use warehouse compute_wh;

create schema frosty_friday.world_bank_metadata;

create or replace table frosty_friday.world_bank_metadata.country_metadata
(
    country_code varchar(3),
    region string,
    income_group string
);

create schema frosty_friday.world_bank_economic_indicators;

create or replace table frosty_friday.world_bank_economic_indicators.gdp
(
    country_name string,
    country_code varchar(3),
    year int,
    gdp_usd double
);

create table frosty_friday.world_bank_economic_indicators.gov_expenditure
(
    country_name string,
    country_code varchar(3),
    year int,
    gov_expenditure_pct_gdp double
);

create schema frosty_friday.world_bank_social_indiactors;

create or replace table frosty_friday.world_bank_social_indiactors.life_expectancy
(
    country_name string,
    country_code varchar(3),
    year int,
    life_expectancy float
);

create or replace table frosty_friday.world_bank_social_indiactors.adult_literacy_rate
(
    country_name string,
    country_code varchar(3),
    year int,
    adult_literacy_rate float
);

create or replace table frosty_friday.world_bank_social_indiactors.progression_to_secondary_school
(
    country_name string,
    country_code varchar(3),
    year int,
    progression_to_secondary_school float
);
