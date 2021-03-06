/*
The possible keys in the bat map are: games, at_bats, hits, runs,
doubles, triples, home_runs, grand_slams, rbis, base_on_balls, ibbs, strikeouts,
sacrifice_hits, sacrifice_flies, hit_by_pitch, gdb, batting_average,
on_base_percentage, and slugging_percentage.
*/

set mapreduce.io.sort.mb 5;

B = LOAD 'baseball/baseball' AS (name:chararray, team:chararray, position:bag{t:(p:chararray)}, bat:map[]);

P = FOREACH B GENERATE FLATTEN(position) AS Position, name AS Player;

Pgroup = GROUP P BY Position;

D = FOREACH Pgroup {
	dist = DISTINCT P.Player;
	GENERATE group, COUNT(dist);
}

dump D;
