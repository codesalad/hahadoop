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

filtered = FILTER salaries BY yearID==1999;
grpd = GROUP filtered BY lgID;
	counts = FOREACH grpd GENERATE group, COUNT(filtered);
	store counts into 'q3_results';
