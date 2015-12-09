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


filteryear = FILTER salaries BY yearID == 1985;
grpd = GROUP filteryear BY teamID;
proj = FOREACH grpd GENERATE group, MIN(filteryear.salary);
filterproj = FILTER proj BY $1 > 100000; 
proj2 = FOREACH filterproj GENERATE $0;

store proj2 into 'q1_results';
