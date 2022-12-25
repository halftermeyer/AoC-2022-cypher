:param env => 'test';

CREATE INDEX Snafu_snafu_decimal
IF NOT EXISTS
FOR (s:Snafu) ON (s.snafu, s.decimal);

CALL apoc.periodic.iterate("MATCH (n) RETURN n",
"DETACH DELETE n",{batchSize:10000});

WITH {`=`:-2, `-`:-1, `0`:0, `1`:1,`2`:2} AS syms
LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH split(line[0], "") AS num, syms
WITH [x IN num | syms[x]] AS num
WITH reduce (acc = 0, n in num | acc * 5 + n) AS num
WITH sum(num) AS total
CREATE (:Total {decimal:total, toProcess:total, snafu:""});

CALL apoc.periodic.commit('
WITH [{sym:"0", val:0},{sym:"1", val:1},{sym:"2", val:2},
  {sym:"=", val:-2},{sym:"-", val:-1}] AS syms
MATCH (t:Total)
WITH t, t.toProcess % 5 AS mod, syms
WITH  (t.toProcess - syms[mod].val)/5 AS new_toProcess,
  syms[mod].sym+t.snafu AS new_snafu, t
SET t.toProcess = new_toProcess, t.snafu = new_snafu
WITH t.toProcess AS limit
RETURN limit', {});

MATCH (t:Total) RETURN t.snafu AS `part 1`;
