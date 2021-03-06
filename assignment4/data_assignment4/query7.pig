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
                    
batting = 	
LOAD 'Batting.csv'
USING PigStorage(',')
AS	
(
playerID:	chararray,	-- Player ID code
yearID:		int,		-- Year
stint:		int,		-- player's stint
teamID:		chararray,	-- Team
lgID:		chararray,	-- League
G:		int,		-- Games
G_batting:	int,		-- Game as batter
AB: 		int,		-- At Bats
R: 		int,		-- Runs
H: 		int,		-- Hits
doubles: 	int,		-- Doubles
triples: 	int,		-- Triples
HR: 		int,		-- Homeruns
RBI: 		int,		-- Runs Batted In
SB: 		int,		-- Stolen Bases
CS: 		int,		-- Caught Stealing
BB: 		int,		-- Base on Balls
SO: 		int,		-- Strikeouts
IBB: 		int,		-- Intentional walks
HBP: 		int,		-- Hit by pitch
SH: 		int,		-- Sacrifice hits
SF: 		int,		-- Sacrifice flies
GIDP: 		int		-- Grounded into double plays
);

salaries_fyear= FILTER salaries BY yearID == 2001;
batting_fyear = FILTER batting BY yearID == 2001;

salaries_fsal = FILTER salaries_fyear BY salary > 500000;
batting_fhr = FILTER batting_fyear BY HR > 50;

proj_sal = FOREACH salaries_fsal GENERATE playerID, salary;
proj_batting = FOREACH batting_fhr GENERATE playerID, HR;

joined = JOIN proj_batting BY playerID, proj_sal BY playerID;

proj_joined = FOREACH joined GENERATE $0, $1, $3;

store proj_joined into 'q7_results';
