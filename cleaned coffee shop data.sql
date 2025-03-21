-- objectives 
	-- clean the data 
-- Data profiling
   -- How many transactions were recored 
    -- date range of sales 
    -- what product and product categories were sold
-- add revenue column
-- add columns to calculate month and day of the week based on trans date 
-- get month column to read 3 letter month instead of numerical month
-- add new column to get the hour from the trans time 
        
        
select *
from sales;
-- updated transaction_id column name 
alter table sales
rename column `ï»¿transaction_id` to transaction_id;

select *
from sales;

-- checked for duplicates 
SELECT *,
row_number() OVER(
partition by transaction_id, transaction_date, transaction_time, transaction_qty, store_id, 
store_location, product_id, unit_price, 
product_category, product_type, product_detail) as row_num
from sales;

with duplicate_cte as
(
SELECT *,
row_number() OVER(
partition by transaction_id, transaction_date, transaction_time, transaction_qty, store_id, 
store_location, product_id, unit_price, 
product_category, product_type, product_detail) as row_num
from sales
)
select*
from duplicate_cte
where row_num >1;

select *
from sales;

-- update transaction_date to date data type 
select `transaction_date`,
str_to_date(`transaction_date`, '%m/%d/%Y')
from sales;

update sales
set `transaction_date` = str_to_date(`transaction_date`, '%m/%d/%Y')
;
alter table sales
modify column `transaction_date` date;

UPDATE sales
SET transaction_time = str_to_date(transaction_time, '%H:%i:%s');

ALTER TABLE sales
MODIFY COLUMN transaction_time time;

select *
from sales;

-- Data Profiling 
	
    -- How many transactions were recored, Count all gives the same but this will leave a note 
		-- for anyone looking at the code later to see what question I was trying to answer. 
select count(transaction_id)
from sales;

 -- date range of sales we have 6 months of data ranging from jan 1 to june 30 
 select min(transaction_date) AS first_date, max(transaction_date) AS last_date
 from sales;
 
   -- what product categories were sold
   
   select *
   from sales;
   
   select Distinct product_category
   from sales; 
 
-- What product were sold 
select distinct product_type
from sales;

-- add revenue column, set the decimal place to 2 and updated so that any future columns added
	-- would calculate revenue as well. 

select *
from sales
limit 2;

alter table sales 
add column Revenue Decimal(10,2);

update sales 
set revenue = unit_price * transaction_qty;

-- add columns to calculate month based on trans date 
alter table sales 
add column month varchar(3);

update sales
set `month` = date_format(transaction_date, '%b')
;

select *
from sales;

--  Add column to calculate the day of the week based on trans date 

alter table sales 
add column Day varchar(3);

update sales 
set Day = date_format(transaction_date, '%a');

select *
from sales;

-- add new column to get the hour from the trans time 

alter table sales
add column Hour int;

update sales
set Hour = hour(transaction_time);


-- Addtional questions to asked 
	-- Total sales for a specific month for all store loactions 

select store_location, concat(round(sum(unit_price * transaction_qty) /1000,1),'K') AS Total_sales
from sales
where month (transaction_date) = 5 -- May 
group by store_location
order by sum(unit_price * transaction_qty) ;


--  query that gives total sales by day labeled based on specific criteria     

select day_of_month,
case
	WHEN total_sales > avg_sales Then 'Above Average'
    When total_sales < avg_sales Then 'Below Average'
    else 'Average'
    End AS sales_status, total_sales
FROM(
	select DAY(transaction_date) AS day_of_month,
	sum(unit_price * transaction_qty) AS total_sales,
    AVG(sum(unit_price * transaction_qty)) Over () AS avg_sales
FROM sales
WHERE month(transaction_date) = 5
group by day(transaction_date))  AS sales_data
order by day_of_month;

select *
from sales;







