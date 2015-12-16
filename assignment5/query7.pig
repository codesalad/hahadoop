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
MPP = LOAD 'baseball/majorleague-payroll2.csv' using PigStorage(',');

F = FILTER MP BY $5 > '"$3';
FF = FILTER MPP BY $4 > '"$3';

J = JOIN F BY $1, FF BY $0;
JJ = JOIN B BY $1, J BY $1;

projection = FOREACH JJ GENERATE $0;
dump projection;
