/*
The possible keys in the bat map are: games, at_bats, hits, runs,
doubles, triples, home_runs, grand_slams, rbis, base_on_balls, ibbs, strikeouts,
sacrifice_hits, sacrifice_flies, hit_by_pitch, gdb, batting_average,
on_base_percentage, and slugging_percentage.
*/

set mapreduce.io.sort.mb 5;

B = LOAD 'baseball/baseball' AS (name:chararray, team:chararray, position:bag{t:(p:chararray)}, bat:map[]);
P = GROUP B BY name;
FLT = FOREACH P GENERATE group AS name, FLATTEN(B.bat) AS Bat;

-- Calculate AVG x 
A = FOREACH FLT GENERATE name, (int)Bat#'games' AS games, (int)Bat#'strikeouts' AS strikeouts;
BB = GROUP A ALL;
AVGX = FOREACH BB generate (float) SUM(A.strikeouts) / SUM(A.games) as avgx;

Players_Averagex = CROSS B, AVGX;

-- Use UDF
register udf_pitchers.jar;
Grouped = GROUP Players_Averagex BY $0;
Filtered = FILTER Grouped BY UdfPitchers(Players_Averagex);

Result = FOREACH Filtered GENERATE $0;

dump Result;
