set mapreduce.io.sort.mb 5;

B = LOAD 'baseball/baseball' AS (name:chararray, team:chararray, position:bag{t:(p:chararray)}, bat:map[]);

C = GROUP B BY team;

D = FOREACH C GENERATE group as Team, FLATTEN(B.bat) AS Bat;

E = FOREACH D GENERATE Team, (int)Bat#'games';

F = GROUP E BY Team;

G = FOREACH F GENERATE group, AVG($1.$1);

DUMP G;

