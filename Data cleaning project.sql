-- Steps for cleaning the data Only work off staging tables never off raw data.  
-- 1. Remove duplicates 
-- 2. standardize the data 
-- 3. null values or blanks values 
-- 4. remove any columns  

create table layoffs_staging2
like layoffs;

select *
from layoffs_staging2;

insert layoffs_staging2
select*
from layoffs;

select *
from layoffs_staging2;

-- Step 1. removing duplicates 

-- adding a filter column where duplicates are greater than 1 for easy identification and removeal. 
select *,
row_number() over(
partition by company, industry, total_laid_off, 'date') as row_num
from layoffs_staging2
;


-- duplicate cte to give only rows that are duplicated values
with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging2
)
select*
from duplicate_cte
where row_num >1;

-- create table based off cte row findings 
CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- new table with filterable column for row_num  
select *
from layoffs_staging3;

insert into layoffs_staging3
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging2;


-- check query 
select *
from layoffs_staging3
where company = 'better.com';

-- filtered results query for duplicates (greater than 1) select statement verifies data to be deleted 
select * 
from layoffs_staging3;


-- delete code based off verified select statment
delete 
from layoffs_staging3
where row_num > 1;


-- standardizing data 

-- view of company column vs trim column 
select company, trim(company)
from layoffs_staging3;

-- trim update code for specific column
update layoffs_staging3
set company = trim(company);

-- verification that trim was executed 
select *
from layoffs_staging3;

-- view for next column to be worked in a-z order identified crypto, crypto currency needs to be merged to one industry. 
select distinct industry
from layoffs_staging3
order by 1;

-- isolating all industry types based on name (crypto) 
select *
from layoffs_staging3
where industry like 'crypto%';

-- update code to change all industry's with crypto to one name type. 
update layoffs_staging3
set industry = 'Crypto'
where industry like 'crypto%';

-- update verification for data merge
select distinct industry
from layoffs_staging3;

select distinct country 
from layoffs_staging3
where country like 'united states%';

-- trailing trim to get rid of the '.' in United States this views the change to make sure the update will run correclty

select distinct country, trim(trailing '.' from country)
from layoffs_staging3
order by 1
;

-- trim update code 
update layoffs_staging3
set country = trim(trailing '.' from country)
where country like 'United States%';

select*
from layoffs_staging3;

-- changing date from text to date, string to date code 

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging3;

-- date update
update layoffs_staging3
set `date` = str_to_date(`date`, '%m/%d/%Y')
;

-- alter table only on staging data (changing date type from text to date type)
alter table layoffs_staging3
modify column `date` date;

select *
from layoffs_staging3;

-- working through nulls and blank values 
select *
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null
;

select * 
from layoffs_staging3
where industry is null 
or industry = '';

select *
from layoffs_staging3
where company = 'airbnb';



-- populating blank industry with correct data based on same company name (industry 'travel' for airbnb but blank for same company on diffrent row)
select t3.industry, t4.industry
from layoffs_staging3 t3
join layoffs_staging3 t4
	on t3.company = t4.company
where (t3.industry is null or t3.industry = '') 
and t4.industry is not null;

-- update blanks to nulls 
update layoffs_staging3
set industry = null
where industry ='';

-- update code to get the industry to populte for the blank or null cells based on same company name

update layoffs_staging3 t3
join layoffs_staging3 t4
		on t3.company = t4.company
set t3.industry = t4.industry
where t3.industry is null 
and t4.industry is not null;


select*
from layoffs_staging3;

-- delete rows only from staging data never from raw data 

delete
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;

-- delete columns from staging data never from raw data 
alter table layoffs_staging3
drop column row_num;

select *
from layoffs_staging3




