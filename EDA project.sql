-- Expolatory Data Analysis (EDA)

select *
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

-- if we order by funcs_raised_millions we can see how big some of these companies were
select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- Companies with the biggest single Layoff
select company, total_laid_off
from world_layoffs.layoffs_staging
order by 2 desc
limit 5;

-- Companies with the most Total Layoffs
select company, sum(total_laid_off)
from layoffs_staging2
group by  company
order by 2 desc
limit 10;

select min(`date`), max(`date`)
from layoffs_staging2;

-- by location
select location, SUM(total_laid_off)
from layoffs_staging2
group by location
order by 2 desc
limit 10;

-- this it total in the past 3 years or in the dataset
select industry, sum(total_laid_off)
from layoffs_staging2
group by  industry
order by 2 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by  year(`date`)
order by 2 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by  stage
order by 2 desc;

select company, avg(percentage_laid_off)
from layoffs_staging2
group by  company
order by 2 desc;

select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

with rolling_total as
(select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off, sum(total_off) over(order by `month`) as rolling_total
from rolling_total;

select company, avg(percentage_laid_off)
from layoffs_staging2
group by  company
order by 2 desc;

select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by  company, year(`date`)
order by 3 desc;

-- Now let's look at that per year
with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by  company, year(`date`)
), 
company_year_ranking as
(select *,  
dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
) 
select company, years, total_laid_off, ranking
from company_year_ranking
where ranking <= 3
and years is not null
order by years asc, total_laid_off desc;

-- Rolling total of layoffs per month
select substring(date,1,7) as dates, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by dates
order by dates asc; 

-- now use it in a CTE so we can query off of it
with date_cte as
(
select substring(date,1,7) as dates, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by dates
order by dates asc
)
select dates, sum(total_laid_off) over(order by dates asc) as rolling_total_layoffs
from date_cte
where dates is not null
order by dates asc;