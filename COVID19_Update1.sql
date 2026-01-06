 
  
 /* Covid-19 Data Exploration Project

The skills used: Bulk insert, Joins, CTE`s, Temp Tables, Windows Functions, Aggrigate Functions, Creating views, Converting Data Types.

*/

/*
----------------------------------------------------------------------------------------------------------------------------------------
-- Bulk insert---
-- in order to be able to insert csv file we need to create a table that can store the file

Create Table COVID19_Deaths
(
code nvarchar(100),
continent nvarchar(255),
country nvarchar(255),
dates nvarchar(255),
populations real, 
total_cases real, 
new_cases real,
new_cases_smoothed real,
total_cases_per_million real,
new_cases_per_million real,
new_cases_smoothed_per_million real,
total_deaths real,
new_deaths real,
new_deaths_smoothed real,
total_deaths_per_million real,
new_deaths_per_million real, 
new_deaths_smoothed_per_million real,
excess_mortality real,
excess_mortality_cumulative real,
excess_mortality_cumulative_absolute real,
excess_mortality_cumulative_per_million real,
hosp_patients real,
hosp_patients_per_million real,
weekly_hosp_admissions real,
weekly_hosp_admissions_per_million real,
icu_patients real,
icu_patients_per_million real,
weekly_icu_admissions real,
weekly_icu_admissions_per_million real,
stringency_index real, 
reproduction_rate real
)

-- After creating table, that match the cvs file, we can use Bulk insert.



BULK INSERT dbo.COVID19_Deaths
FROM 'C:\Data_Analyst\Portfolio\Covid_19\Death19.csv'
WITH (
		FIELDTERMINATOR = '|',
		ROWTERMINATOR = '\n',
		FIRSTROW = 2,
		TABLOCK
	 )


-- We need to check if the Bulk insert is worked properly. 

SELECT *
FROM dbo.COVID19_Deaths

-- Inspecting the Table 

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'COVID19_Deaths';


-- IF the Bulk insert is failed or worked in a wrong way we can delete the table we inserted in. 


Drop Table If Exists COVID19_Deaths --(use only if the Bulk insert failed or not right)



-- Same for the second table 

Drop Table If Exists COVID19_Vassinations

Create Table COVID19_Vassinations
(
code nvarchar(255),
continent nvarchar (255),
country nvarchar(255),
dates nvarchar(255),
total_tests real,
new_tests real, 
total_tests_per_thousand real, 
new_tests_per_thousand real, 
new_tests_smoothed real,
new_tests_smoothed_per_thousand real, 
positive_rate real, 
tests_per_case real,
total_vaccinations real, 
people_vaccinated real, 
people_fully_vaccinated real, 
total_boosters real, 
new_vaccinations real, 
new_vaccinations_smoothed real, 
total_vaccinations_per_hundred real, 
people_vaccinated_per_hundred real, 
people_fully_vaccinated_per_hundred real, 
total_boosters_per_hundred real, 
new_vaccinations_smoothed_per_million real, 
new_people_vaccinated_smoothed real, 
new_people_vaccinated_smoothed_per_hundred real, 
population_density real, 
median_age real, 
life_expectancy real, 
gdp_per_capita real, 
extreme_poverty real, 
diabetes_prevalence real, 
handwashing_facilities real, 
hospital_beds_per_thousand real,
human_development_index real  
)


BULK INSERT dbo.COVID19_Vassinations
FROM 'C:\Data_Analyst\Portfolio\Covid_19\Vaccinations19.csv'
WITH (
		FIELDTERMINATOR = '|',
		ROWTERMINATOR = '\n',
		FIRSTROW = 2,
		TABLOCK
	 )


 select*
from COVID19_Vassinations


SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'COVID19_Vassinations';

*/

-----------------------------------------------------
--DATA CLEANING-- 

-- Checking for dublicates

SELECT code, continent,dates, country, populations, COUNT(*) AS Duplicates
FROM COVID19_Deaths
GROUP BY code, continent, dates, country, populations
HAVING COUNT(*) > 1;

-- Checking for the nulls

SELECT *
FROM COVID19_Deaths
WHERE continent is null


--- Deleting the nulls from continent values. 

DELETE FROM COVID19_Deaths
WHERE populations is null
or continent is null

SELECT *
FROM COVID19_Deaths
Where dates is null

DECLARE @sql NVARCHAR(MAX) = '';
SELECT @sql = STRING_AGG('UPDATE COVID19_Deaths SET ' + name + ' = COALESCE( ' + name + ', 0);',
						 ' ')
FROM sys.columns 
WHERE object_id = OBJECT_ID('COVID19_Deaths')
 AND system_type_id IN (56, 127, 62, 108, 106, 59)

 EXEC sp_executesql @sql;

 SELECT*
 FROM COVID19_Deaths


--Transformning column data type

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'COVID19_Deaths';

--dates column is nvarchar can be changed to dates 
--total_cases, new_cases, total_deaths, new_deaths, populations, can be changed to bigint 

-- in order to avoid problems dureing transformation
-- add new columns 

UPDATE COVID19_Deaths 
SET dates = TRY_CAST(dates AS DATE)
WHERE TRY_CAST(dates AS DATE) is null

ALTER TABLE COVID19_Deaths
ALTER COLUMN dates date
	

Select *
From SQLportfolio.dbo.COVID19_Deaths
order by 3,4

-- repeat for the second table 
--dublicates

SELECT code, continent,dates, country, COUNT(*) AS Duplicates
FROM COVID19_Vassinations
GROUP BY code, continent, dates, country
HAVING COUNT(*) > 1;

--nulls 
SELECT *
FROM COVID19_Vassinations
WHERE continent is null 

DELETE FROM COVID19_Vassinations
WHERE continent is null


DECLARE @sql NVARCHAR(MAX) = '';
SELECT @sql = STRING_AGG('UPDATE COVID19_Vassinations SET ' + name + ' = COALESCE( ' + name + ', 0);',
						 ' ')
FROM sys.columns 
WHERE object_id = OBJECT_ID('COVID19_Vassinations')
 AND system_type_id IN (56, 127, 62, 108, 106, 59)

 EXEC sp_executesql @sql;

-- altering the dates columns data type 
 
UPDATE COVID19_Vassinations
SET dates = TRY_CAST(dates AS DATE)
WHERE TRY_CAST(dates AS DATE) is null

ALTER TABLE COVID19_Vassinations
ALTER COLUMN dates date
	

select*
FROM COVID19_Vassinations

-- Select Data that we are going to be starting use.

Select country, dates,total_cases, new_cases, total_deaths, populations 
From SQLportfolio.dbo.COVID19_Deaths
Where continent is not null
order by 1,2


-- Total Cases versus Total Deaths
-- This will show the mortality rate of the Virus in specific country. 

Select country, dates, total_cases, total_deaths,(total_deaths/total_cases)*100 as mortality_rate
FROM SQLportfolio.dbo.COVID19_Deaths
Where total_cases > 0 and country like '%%Uzbek%%' and continent is not null
order by 1,2

-- Total cases versus Populations
-- This shows the the infection rate of the population.

Select country, dates, total_cases, populations,(total_cases/populations)*100 AS infection_rate
FROM SQLportfolio.dbo.COVID19_Deaths
Where continent is not null  -- and country like '%%Uzbek%%'
order by 1,2



-- Contries with highest infection rate compared to population

SELECT country, populations, MAX(total_cases) AS HighestInfectionCount, MAX(total_cases/populations)*100 AS InfectedPopulationPercentage 
FROM SQLportfolio.dbo.COVID19_Deaths
WHERE continent IS NOT NULL
GROUP BY country,populations
ORDER BY InfectedPopulationPercentage desc


-- Countries with highest deaths compared to population

SELECT	country, MAX(CAST(total_deaths AS INT)) AS Total_Death_Count 
FROM SQLportfolio.dbo.COVID19_Deaths
Where continent	IS NOT NULL
GROUP BY country
ORDER BY Total_Death_Count DESC


/*
-- Replacing the null to 0 in total_deaths column.

SELECT total_deaths
FROM SQLportfolio.dbo.COVID19_Deaths
Where total_deaths is null

UPDATE SQLportfolio.dbo.COVID19_Deaths
SET total_deaths = 0
WHERE total_deaths is null
*/

--BREAKING THING DOWN BY CONTINENT

--Showing continents with highest death count per population

SELECT continent, MAX(CAST(total_deaths AS INT)) AS TotalDeathCount
FROM SQLportfolio.dbo.COVID19_Deaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY TotalDeathCount

-- GLOBAL NUMBERS

SELECT SUM(new_cases) AS total_cases, SUM(CAST(new_deaths AS INT)) AS total_deaths, SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100 AS DeathPercentage
FROM SQLportfolio.dbo.COVID19_Deaths
WHERE continent IS NOT NULL
ORDER BY 1,2


-- Total Population versus Vaccinations
-- Shows the percentage of the population that recieved at least one vaccination

UPDATE SQLportfolio.dbo.COVID19_Vassinations
SET new_vaccinations = 0
WHERE new_vaccinations IS NULL


SELECT death.continent, death.country, death.dates, death.populations, vacc.new_vaccinations,
SUM(CONVERT(BIGINT,vacc.new_vaccinations)) OVER (PARTITION BY death.country  ORDER BY death.country, death.dates) AS RollingPeopleVaccinated
FROM SQLportfolio.dbo.COVID19_Deaths death
JOIN SQLportfolio.dbo.COVID19_Vassinations vacc
	ON death.country = vacc.country
	AND death.dates = vacc.dates
WHERE death.continent IS NOT NULL
ORDER BY 2,3


--Using CTE to perform Calculation on Partiton by in previous query

WITH PopvsVac (continent, country, dates, populations, new_vaccinations, RollingPeopleVaccinated)
AS
(
	SELECT death.continent, death.country, death.dates, death.populations, vacc.new_vaccinations,
	SUM(CONVERT(BIGINT,vacc.new_vaccinations)) OVER (PARTITION BY death.country  ORDER BY death.country, death.dates) AS RollingPeopleVaccinated
	FROM SQLportfolio..COVID19_Deaths death
	JOIN SQLportfolio..COVID19_Vassinations vacc
		ON death.country = vacc.country
		AND  death.dates = vacc.dates
	WHERE death.continent IS NOT NULL
)
SELECT*,(RollingPeopleVaccinated/populations)*100 AS VaccRate
FROM PopvsVac
Where country like '%%state%%'
order by 2,3


-- Using temp Tables to perfom calculation on vaccinations counting

DROP TABLE IF EXISTS #VaccinatedPopulationPercentage
CREATE TABLE #VaccinatedPopulationPercentage
(
	Continent nvarchar(255),
	Country nvarchar(255),
	Date datetime,
	Population numeric,
	New_vaccinations numeric,
	RollingPeopleVaccinated numeric
)

INSERT INTO #VaccinatedPopulationPercentage
SELECT death.continent, death.country, death.dates, death.populations, vacc.new_vaccinations,
	SUM(CONVERT(BIGINT,vacc.new_vaccinations)) OVER (PARTITION BY death.country  ORDER BY death.country, death.dates) AS RollingPeopleVaccinated
	FROM SQLportfolio..COVID19_Deaths death
	JOIN SQLportfolio..COVID19_Vassinations vacc
		ON death.country = vacc.country
		AND  death.dates = vacc.dates
	--WHERE death.continent IS NOT NULL

SELECT*,(RollingPeopleVaccinated/Population)*100
FROM #VaccinatedPopulationPercentage
Where Continent IS NOT NULL
ORDER BY 2,3


CREATE View VaccinatedPopulationPrecentage AS
SELECT death.continent, death.country, death.dates, death.populations, vacc.new_vaccinations,
	SUM(CONVERT(BIGINT,vacc.new_vaccinations)) OVER (PARTITION BY death.country  ORDER BY death.country, death.dates) AS RollingPeopleVaccinated
	FROM SQLportfolio..COVID19_Deaths death
	JOIN SQLportfolio..COVID19_Vassinations vacc
		ON death.country = vacc.country
		AND  death.dates = vacc.dates
		WHERE death.continent IS NOT NULL


