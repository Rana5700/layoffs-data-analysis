-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022



select * from world_layoffs.layoffs ;



-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens
Create table layoffs_staging 
like layoffs; 


insert layoffs_staging 
Select * From layoffs ;

select * from layoffs_staging ;



-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates

# First let's check for duplicates



 with Duplicate_cte AS
 (
 select * ,
row_number() over(
partition by company ,location, industry ,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
 from layoffs_staging 
 
 )

select * FROM Duplicate_cte
where row_num > 1 ;



 with Duplicate_cte AS
 (
 select * ,
row_number() over(
partition by company ,location, industry ,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
 from layoffs_staging 
 
 )

delete FROM Duplicate_cte
where row_num > 1 ;






create table layoffs_staging2
like layoffs_staging ;

alter table layoffs_staging2
add row_num int ;

select * from layoffs_staging2 ;



insert into layoffs_staging2
 select * ,
row_number() over(
partition by company ,location, industry ,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
 from layoffs_staging ;
 
delete from layoffs_staging2 
where row_num > 1 ;

select * from layoffs_staging2 ;


-- 2. Standardize Data

select company ,trim(company) 
from layoffs_staging2 ;

update layoffs_staging2
 SET company = trim(company) ;


select distinct industry
from layoffs_staging2 
ORDER BY 1 ;


select *
from layoffs_staging2 
WHERE industry  like 'Crypto%' ;


update layoffs_staging2 
SET industry = 'Crypto'
where industry like 'Crypto %' ;

select distinct industry
 from layoffs_staging2 ;


update layoffs_staging2 
SET country = 'United states'
where country like 'United states % ' ;

SELECT TRIM(`date`) 
FROM layoffs_staging2;

SELECT `date`,
       STR_TO_DATE(`date`, '%m/%d/%Y') 
FROM layoffs_staging2;


update layoffs_staging2
SET  `date` = STR_TO_DATE(`date`, '%m/%d/%Y') ;

alter table layoffs_staging2
modify column `date` date ;

-- 3. Null Values or Blank Values 

update layoffs_staging2 
set industry = null
where industry ='' ;


select *
from layoffs_staging2
where company = 'Airbnb';
 
 select *
 from layoffs_staging2
 where industry is null
 or industry =' '
;

SELECT t1.*
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
AND t1.industry IS NULL
AND t2.industry IS NOT NULL;

update layoffs_staging2 AS t1
join layoffs_staging2 AS t2
on t1.company = t2.company 
set t1.industry = t2.industry
where t1.industry IS NULL 
AND t2.industry IS NOT NULL;

select * from layoffs_staging2
WHERE total_laid_off IS NULL
AND  percentage_laid_off IS NULL ;
 

-- 4. Remve Any Columns or row

DELETE
 from layoffs_staging2
WHERE total_laid_off IS NULL
AND  percentage_laid_off IS NULL ;


alter table layoffs_staging2 
DROP column row_num ;

