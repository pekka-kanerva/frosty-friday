-- Week 15 challenge

-- Use Frosty Friday role created in week 1 
use role ff_role;

-- Use database for challenges created in week 1
use database frosty_friday;

-- Set warehouse
use warehouse compute_wh;

-- Create schema for week15 challenge
create schema week15;

-- Create table as given in challenge
create table home_sales (
sale_date date,
price number(11, 2)
);

insert into home_sales (sale_date, price) values
(‘2013-08-01’::date, 290000.00),
(‘2014-02-01’::date, 320000.00),
(‘2015-04-01’::date, 399999.99),
(‘2016-04-01’::date, 400000.00),
(‘2017-04-01’::date, 470000.00),
(‘2018-04-01’::date, 510000.00);

-- Solution
create or replace function get_price_bucket( p_price number(11,2)
                                           , p_bin_ranges varchar)
returns number
as
$$
  with bins as (
select nullif(split_part(split_part(p_bin_ranges,',',1),'-',1),'')::number as bin1_low
     , nullif(split_part(split_part(p_bin_ranges,',',1),'-',2),'')::number as bin1_high
     , nullif(split_part(split_part(p_bin_ranges,',',2),'-',1),'')::number as bin2_low
     , nullif(split_part(split_part(p_bin_ranges,',',2),'-',2),'')::number as bin2_high
     , nullif(split_part(split_part(p_bin_ranges,',',3),'-',1),'')::number as bin3_low
     , nullif(split_part(split_part(p_bin_ranges,',',3),'-',2),'')::number as bin3_high
     , nullif(split_part(split_part(p_bin_ranges,',',4),'-',1),'')::number as bin4_low
     , nullif(split_part(split_part(p_bin_ranges,',',4),'-',2),'')::number as bin4_high
     , nullif(split_part(split_part(p_bin_ranges,',',5),'-',1),'')::number as bin5_low
     , nullif(split_part(split_part(p_bin_ranges,',',5),'-',2),'')::number as bin5_high
     , nullif(split_part(split_part(p_bin_ranges,',',6),'-',1),'')::number as bin6_low
     , nullif(split_part(split_part(p_bin_ranges,',',6),'-',2),'')::number as bin6_high
)
select case 
         when p_price between bin1_low and bin1_high or bin2_low is null then 1
         when p_price between bin2_low and bin2_high or bin3_low is null then 2
         when p_price between bin3_low and bin3_high or bin4_low is null then 3
         when p_price between bin4_low and bin4_high or bin5_low is null then 4
         when p_price between bin5_low and bin5_high or bin6_low is null then 5
         when p_price between bin6_low and bin6_high or p_price > bin6_high then 6
         else 9999
       end as bin
from bins
$$
;

-- Results
select sale_date
     , price
     , get_price_bucket(price, '0-1,2-310000,310001-400000,400001-500000') as bucket_set_1
     , get_price_bucket(price, '0-210000,210001-350000') as bucket_set_2
     , get_price_bucket(price, '0-250000,250001-290001,290002-320000,320001-360000,360001-410000,410001-470001') as bucket_set_3
from home_sales;
