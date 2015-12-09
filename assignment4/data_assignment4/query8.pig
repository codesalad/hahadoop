set mapreduce.io.sort.mb 5;

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

REGISTER udf_percentage.jar;
proj = FOREACH batting GENERATE playerID, UdfPercentage(HR, G); --no package declaration
ordered = ORDER proj BY $1 DESC;
top10 = LIMIT ordered 10;
dump top10;
store top10 into 'q8_results';
