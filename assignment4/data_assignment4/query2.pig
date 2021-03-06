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

filtered = FILTER salaries BY yearID==1998;
grpd = GROUP filtered BY teamID;
avgs = FOREACH grpd GENERATE group, AVG(filtered.salary);
store avgs into 'q2_results';
