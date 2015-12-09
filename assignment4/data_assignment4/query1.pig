set mapreduce.io.sort.mb 5;

salaries = 	
LOAD 'Salaries.csv' 
USING PigStorage(',')
AS 	
(
yearID:		int, 		-- Year
teamID:		chararray, 	-- Team
lgID:		chararray, 	-- League
playerID:	chararray, 	-- Player ID code
salary:		int		-- Salary
);
                    
-- CONTAINS PLAYERS WITH SALARIES > 100000
filtersalary = FILTER salaries BY salary > 100000;
grpf = GROUP filtersalary BY teamID;
countedf = FOREACH grpf GENERATE group AS teamid, COUNT(filtersalary);

-- CONTAINS COUNT OF PLAYERS PER TEAM
grpt = GROUP salaries BY teamID;
countedt = FOREACH grpt GENERATE group AS teamid, COUNT(salaries);

joined = JOIN countedf BY teamid, countedt BY teamid;

filtered = FILTER joined BY $1 == $3;

filtered2 = FOREACH filtered GENERATE $0 AS teamID, $1 AS count;

store filtered2 into 'q1_results';