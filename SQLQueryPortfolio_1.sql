/*

Covid 19 Data Exploration


Skills used: Joins, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types,

*/

--Importing Data through Wizzard was unsuccecfull. Bulk insert was used to import the data.

DROP TABLE IF EXISTS CovidDeaths 
CREATE TABLE CovidDeaths 
(iso_Code varchar(10), 
continent varchar(50), 
locations varchar (50),
dates date,
populations BIGINT,
total_cases int,
new_cases FLOAT NULL,
new_cases_smoothed decimal, 
total_deaths int,
new_deaths int, 
new_deaths_smoothed decimal,
total_cases_per_million decimal,
new_cases_per_million decimal,
new_cases_smoothed_per_million decimal,
total_deaths_per_million decimal,
new_deaths_per_million decimal,
new_deaths_smoothed_per_million decimal,
reproduction_rate decimal,
icu_patients decimal,
icu_patients_per_million decimal,
hosp_patients int,
hosp_patients_per_million decimal,
weekly_icu_admissions FLOAT NULL,
weekly_icu_admissions_per_million decimal,
weekly_hosp_admissions Float Null,
weekly_hosp_admissions_per_million FLOAT NULL
)


USE SQLportfolio;
BULK INSERT CovidDeaths
FROM 'C:\Users\User\Downloads\CovidDeaths.csv'
WITH (
FIELDTERMINATOR = ',',     -- separates columns
  ROWTERMINATOR = '\n',      -- separates rows
  FIRSTROW = 2               -- skip header row
);


 -- Checking if data inserted correctly.


SELECT *
FROM SQLportfolio.dbo.CovidDeaths





DROP TABLE IF EXISTS CovidVaccinations;
CREATE TABLE CovidVaccinations
(
	iso_code varchar(10),
	continent varchar(50),
	locations varchar(50),
	dates DATETIME,
	new_tests int NULL,
	total_tests int NULL,
	total_tests_per_thousand FLOAT NULL,
	new_tests_per_thousand FLOAT NULL,
	new_tests_smoothed int,
	new_test_smoothed_per_thousand FLOAT NULL,
	positive_rate FLOAT NUll,
	tests_per_case FLOAT NULL,
	tests_units varchar(50),
	total_vaccinations int,
	people_vaccinated int,
	people_fully_vaccinated int,
	new_vaccinations int,
	new_vaccinations_smoothed int,
	total_vaccinations_per_hundred FLOAT NULL,
	people_vaccinated_per_hundred FLOAT NULL,
	people_fully_vaccinated_per_hundred FLOAT NULL,
	new_vaccinations_smoothed_per_million int,
	stringency_index FLOAT NULL,
	population_density FLOAT NULL,
	median_age FLOAT NULL,
	aged_65_older FLOAT NULL,
	aged_70_older FLOAT NULL,
	gdp_per_capita FLOAT NUll,
	extreme_poverty FLOAT NULL,
	cardiovasc_death_rate FLOAT NULL,
	diabetes_prevalence FLOAT NULL,
	female_smokers FLOAT NULL,
	male_smokers FLOAT NULL,
	handwashing_facilities FLOAT NULL,
	hospital_beds_per_thousand FLOAT NULL,
	life_expectancy FLOAT NULL,
	human_development_index FLOAT NULL,
)


USE SQLportfolio
BULK INSERT CovidVaccinations
FROM 'C:\Users\User\Downloads\CovidVaccinations.csv'
WITH (
FIELDTERMINATOR = ',',     -- separates columns
  ROWTERMINATOR = '\n',      -- separates rows
  FIRSTROW = 2               -- skip header row
);


SELECT*
FROM SQLportfolio.dbo.CovidVaccinations


--To check the table elements.


Select *
FROM SQLportfolio..CovidDeaths
Where continent is not NULL
ORDER BY 3,4


--Select Data that we are going to start with.


SELECT locations, dates, total_cases, new_cases, total_deaths, populations 
FROM SQLportfolio..CovidDeaths
Order by 1, 2


-- TOTAL CASES vs TOTAL DEATHS.
--Calculating the probabiliy of dying from Covid if infected.


SELECT locations, dates, total_cases,  total_deaths, (CAST(total_deaths as float)/total_cases)*100 as Death_probability
FROM SQLportfolio..CovidDeaths
Where locations like 'Uzbekistan'
Order by 1, 2


-- TOTAL CASES vs POPULATION.
-- What percentage of the population got covid.


SELECT locations, dates, total_cases, populations, (CAST(total_cases AS float)/populations)*100 as Infection_rate
FROM SQLportfolio..CovidDeaths
Where locations = 'Uzbekistan'
Order by 1, 2



-- Countries with Highest Infection Rate compared to population.



SELECT locations, populations, MAX(total_cases) as HighestInfectionRate, MAX((total_cases*1.0/populations)*100) as Max_Infection_Rate 
FROM SQLportfolio..CovidDeaths
Group BY locations, populations
Order by Max_Infection_Rate desc



-- Countries with Highest deaths Counts 



SELECT locations, MAX(total_deaths) as TotalDeathCount
FROM SQLportfolio..CovidDeaths
Where continent is not NUll
Group By locations
Order By TotalDeathCount desc


-- BREAKING THINGS DOWN BY CONTINENT



-- Continents with the highest death count per population


SELECT continent, MAX(total_deaths) as TotalDeathCount
FROM SQLportfolio..CovidDeaths
Where continent is not NUll
Group By continent
Order By TotalDeathCount desc


 -- GLOBAL NUMBERS


SELECT SUM(new_cases) AS Total_cases, SUM(new_deaths) AS Total_deaths,
SUM(CAST(new_deaths as FLOAT))/SUM(new_cases)*100 AS DeathPercentage
FROM SQLportfolio..CovidDeaths
WHERE continent is not null
--Group by dates
Order by 1, 2


-- Total population versus total vaccinations

SELECT dea.continent, dea.locations, dea.dates, dea.populations, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.locations ORDER BY dea.locations, dea.dates)
as RollingPeopleVaccinated
FROM SQLportfolio..CovidDeaths dea
 JOIN SQLportfolio..CovidVaccinations vac
	ON dea.locations = vac.locations 
	AND dea.dates = vac.dates
WHERE dea.continent is not null
ORDER BY 2,3



-- Using CTE to perform Calculation on Partition By in previous query


WITH PopvsVac (Continent, Location, Date, Population,newVacs, RollingPeopleVaccinated)
AS
(
SELECT dea.continent, dea.locations, dea.dates, dea.populations, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.locations ORDER BY dea.locations, dea.dates)
as RollingPeopleVaccinated

FROM SQLportfolio..CovidDeaths dea
 JOIN SQLportfolio..CovidVaccinations vac
	ON dea.locations = vac.locations 
	AND dea.dates = vac.dates
WHERE dea.continent is not null
--ORDER BY 2,3
)
Select*, (CAST(RollingPeopleVaccinated AS float)/Population)*100
From PopvsVac
Order BY 2,3



-- Using Temp Table to perform Calculation on Partition By in previous query

DROP TABLE if EXISTS #VaccinatedPercentage
CREATE TABLE #VaccinatedPercentage
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
NewVacs numeric,
RollingPeopleVaccinated numeric
)
INSERT INTO #VaccinatedPercentage
SELECT dea.continent, dea.locations, dea.dates, dea.populations, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.locations ORDER BY dea.locations, dea.dates)
as RollingPeopleVaccinate

FROM SQLportfolio..CovidDeaths dea
 JOIN SQLportfolio..CovidVaccinations vac
	ON dea.locations = vac.locations 
	AND dea.dates = vac.dates
WHERE dea.continent is not null

-- This will show daily increase percentage of vaccinations per population

Select *, (RollingPeopleVaccinated/Population)*100
From #VaccinatedPercentage

-- THis will show what percentage of population got vaccinated by 2021-04-30

Select *,((MAX(RollingPeopleVaccinated) OVER (PARTITION BY Location ORDER BY Location))/Population)*100 AS VacRate
FROM #VaccinatedPercentage
--where Location like 'Uzb%%'
ORDER BY 2,3



-- Creating view to store date for later visualizations

CREATE VIEW Vacpercentage as 
SELECT dea.continent, dea.locations, dea.dates, dea.populations, vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.locations ORDER BY dea.locations, dea.dates)
as RollingPeopleVaccinated

FROM SQLportfolio..CovidDeaths dea
 JOIN SQLportfolio..CovidVaccinations vac
	ON dea.locations = vac.locations 
	AND dea.dates = vac.dates
WHERE dea.continent is not null
--ORDER BY 2,3
