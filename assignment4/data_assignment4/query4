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

filtered = FILTER batting BY yearID == 1988;
ordered = ORDER filtered BY H DESC;
top = foreach ordered generate playerID, H;
top10 = LIMIT top 10;
store top10 into 'q4_results';
