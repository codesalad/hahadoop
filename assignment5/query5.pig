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

A = FOREACH FLT GENERATE name, (int)Bat#'games' AS games, (int)Bat#'strikeouts' AS strikeouts;
BB = GROUP A ALL;

AVGX = FOREACH BB generate (float) SUM(A.strikeouts) / SUM(A.games) as avgx;

T = CROSS B, AVGX;

register udf_pitchers.jar;

TP = GROUP T BY $0;

TT = FILTER TP BY UdfPitchers(T);

dump TT;
