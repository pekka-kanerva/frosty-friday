-- Week 13 challenge

-- Use Frosty Friday role created in week 1 
use role ff_role;

-- Use database for challenges created in week 1
use database frosty_friday;

-- Set warehouse
use warehouse compute_wh;

-- Create schema for week13 challenge
create schema week13;

-- Create table as given in challenge
create or replace table testing_data(id int autoincrement start 1 increment 1, product string, stock_amount int,date_of_check date);

insert into testing_data (product,stock_amount,date_of_check) values ('Superhero capes',1,'2022-01-01');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero capes',2,'2022-01-02');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero capes',NULL,'2022-02-01');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero capes',NULL,'2022-03-01');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero masks',5,'2022-01-01');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero masks',NULL,'2022-02-13');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero pants',6,'2022-01-01');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero pants',NULL,'2022-01-01');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero pants',3,'2022-04-01');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero pants',2,'2022-07-01');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero pants',NULL,'2022-01-01');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero pants',3,'2022-05-01');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero pants',NULL,'2022-10-01');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero masks',10,'2022-11-01');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero masks',NULL,'2022-02-14');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero masks',NULL,'2022-02-15');
insert into testing_data (product,stock_amount,date_of_check) values ('Superhero masks',NULL,'2022-02-13');


-- Solution using lag with ignore nulls
select product
     , stock_amount
     , nvl(stock_amount, lag(stock_amount) ignore nulls over (partition by product order by date_of_check, id))  as stock_amount_filled_out
     , date_of_check
from testing_data
order by product, date_of_check, id;

-- Alternative solution
with all_rows_prev_date_added as 
(
select t.id
     , t.product
     , t.stock_amount
     , t.date_of_check
     , (select max(t2.date_of_check) 
           from testing_data t2
           where t2.product = t.product 
           and t2.stock_amount is not null 
           and t2.date_of_check <= t.date_of_check) as prev_date
from testing_data t     
)
select a.id
     , a.product
     , nvl(a.stock_amount, not_nulls.stock_amount) stock_amount_filled_out
     , a.date_of_check
from   all_rows_prev_date_added a
,      testing_data not_nulls
where  a.product = not_nulls.product
and    a.prev_date = not_nulls.date_of_check
and    not_nulls.stock_amount is not null
order by a.product, a.date_of_check, a.id;
