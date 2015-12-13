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

Pfilter = FILTER FLT BY (int)Bat#'games' > 100;

PfilterR = FOREACH FLT	GENERATE $0, (float)$1#'home_runs' / $1#'games' AS R;

sorted = ORDER PfilterR BY R DESC;

DUMP sorted;
