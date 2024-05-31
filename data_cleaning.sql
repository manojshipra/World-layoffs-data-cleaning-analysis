-- DATA CLEANING

SELECT * 
FROM layoffs
;

-- DO not direclty maippulate or query the raw dataset, make a copy or staging dataset and then move forward
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging 
SELECT * 
FROM layoffs
;

SELECT * 
FROM layoffs_staging
;
-- OR 
CREATE TABLE layoffs_staging2
SELECT * 
FROM layoffs
;

SELECT * 
FROM layoffs_staging2
;

-- CHECK for duplicate rows in the dataset 
SELECT * , ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
; 

-- A common expression table to check for duplicate rows, row_num>1 are the duplicates
WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company,location, industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num>1;

-- creating a new table with a new column 'row_num'

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company,location, industry, total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging 
;

-- deleting the duplicate rows in the new table 

DELETE
FROM layoffs_staging2
WHERE row_num>1;


-- Standardizing data

SELECT distinct(TRIM(company))
from layoffs_staging2
;

UPDATE layoffs_staging2
SET company=TRIM(company)
;

UPDATE layoffs_staging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%'
;

SELECT DISTINCT(location)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

-- eliminating some common mistakes in the dataset like spelling mistakes etc..

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country=TRIM(TRAILING '.' FROM country)
WHERE country LIKE "%.";
;

-- converting the date column to DATE datatype 

SELECT `date`, STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`=STR_TO_DATE(`date`,"%m/%d/%Y")
;

-- changing the datatype
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
and percentage_laid_off IS NULL
;

SELECT *
from layoffs_staging2
WHERE industry IS NULL 
OR industry='';

-- populating the rows where the company has an industry present in some rows and is null in some other rows

SELECT * 
FROM layoffs_staging2 st1
JOIN layoffs_staging2 st2
	ON st1.company=st2.company
    AND st1.location=st2.location
WHERE (st1.industry is NULL OR st1.industry='')
AND st2.industry is not null
;

UPDATE layoffs_staging2
SET industry=NULL
WHERE industry=''
;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE (t1.industry is NULL OR t1.industry='')
AND t2.industry is not null
;

-- removing the rows where the data is not useful
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
and percentage_laid_off IS NULL
;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
and percentage_laid_off IS NULL
;

ALTER TABLE layoffs_staging2
DROP column row_num
;

SELECT * 
FROM layoffs_staging2
;
