-- DATA CLEANING --

SELECT * 
FROM world_layoffs.layoffs;

-- 1. Remove Duplicates 
-- 2. Standerdize the data
-- 3. Null values & Blank values 
-- 4 . Remove any Colame & Row 


CREATE TABLE layeoffs_staging
LIKE layoffs;

SELECT * 
FROM world_layoffs.layeoffs_staging;

INSERT INTO layeoffs_staging 
SELECT * 
FROM layoffs;


SELECT *,
ROW_NUMBER() OVER(PARTITION BY company , industry, location , total_laid_off, `date`) AS row_num
 FROM layeoffs_staging;
 
 WITH dublicate_CTE AS 
 (
 SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company , industry, location , total_laid_off, `date`, stage , country , funds_raised_millions 
) AS row_num
 FROM layeoffs_staging
)
SELECT * 
FROM dublicate_CTE
WHERE  row_num > 1;

SELECT * 
FROM layeoffs_staging
WHERE company = 'Casper';


CREATE TABLE `layeoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num`  INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layeoffs_staging2;

INSERT INTO layeoffs_staging2
 SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company , industry, location , percentage_laid_off,total_laid_off, `date`, stage , country , funds_raised_millions 
) AS row_num
 FROM layeoffs_staging;


DELETE 
FROM layeoffs_staging2
WHERE row_num > 1;


SELECT *  
FROM layeoffs_staging2
WHERE row_num >1;


-- Standerdizing data i.e finding issues in data & fixing it --


SELECT company , (TRIM(company)) 
FROM layeoffs_staging2;


UPDATE layeoffs_staging2
SET 
company = (TRIM(company));


SELECT DISTINCT industry 
FROM layeoffs_staging2
WHERE industry LIKE 'Crypto%';



UPDATE layeoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT country
FROM layeoffs_staging2
WHERE country LIKE 'United States%';


SELECT DISTINCT country , TRIM(TRAILING '.' FROM country)
FROM layeoffs_staging2
ORDER BY 1;
-- TRAILING coming at the end --
WITH demo AS (
SELECT DISTINCT country , TRIM(TRAILING '.' FROM country)

FROM layeoffs_staging2
ORDER BY 1
)
SELECT country , TRIM(TRAILING 'a' FROM country)
FROM demo;



UPDATE layeoffs_staging2
SET country = TRIM(TRAILING '.' FROM country )
WHERE country LIKE 'United States%';



SELECT `date`, STR_TO_DATE(`date` , '%m/%d/%Y')
FROM layeoffs_staging2;


UPDATE layeoffs_staging2
SET `date` = STR_TO_DATE(`date` , '%m/%d/%Y');


ALTER TABLE layeoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * 
FROM layeoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layeoffs_staging2
SET industry = null
WHERE industry = '';


SELECT * 
FROM layeoffs_staging2
WHERE industry IS NULL 
OR industry ='' ;

SELECT * 
FROM layeoffs_staging2
WHERE company =  "Bally's Interactive";

UPDATE layeoffs_staging2
SET industry = 'Travel'
WHERE company = 'Airbnb';

SELECT *
FROM layeoffs_staging2 t1
JOIN layeoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.company IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layeoffs_staging2 t1
JOIN layeoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


SELECT * 
FROM layeoffs_staging2;




SELECT * 
FROM layeoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layeoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


ALTER TABLE layeoffs_staging2
DROP COLUMN row_num
















