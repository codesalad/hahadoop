/*
The possible keys in the bat map are: games, at_bats, hits, runs,
doubles, triples, home_runs, grand_slams, rbis, base_on_balls, ibbs, strikeouts,
sacrifice_hits, sacrifice_flies, hit_by_pitch, gdb, batting_average,
on_base_percentage, and slugging_percentage.

RANK,TEAM,TOTAL PAYROLL,AVG SALARY,MEDIAN SALARY,STANDARD DEV

Team,Payroll,Averge,Median
*/

B = LOAD 'baseball/baseball' AS (name:chararray, team:chararray, position:bag{t:(p:chararray)}, bat:map[]);
MP = LOAD 'baseball/majorleague-payroll.csv' using PigStorage(',');

filtered = FILTER MP BY $5 > '"$3';

joined = JOIN B by $1, filtered by $1;

projection = FOREACH joined GENERATE $0;
dump projection;
