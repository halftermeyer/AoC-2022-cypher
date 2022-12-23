:param env => 'input';

MATCH (n) DETACH DELETE n;

// PARSE MAP

CREATE CONSTRAINT Position_x_y
IF NOT EXISTS
FOR (p:Position) REQUIRE (p.x, p.y) IS NODE KEY;

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH collect(line) AS lines
WITH [ix IN range (0, size(lines)-1)| {y:ix+1, row:lines[ix][0]}] AS lines
UNWIND lines AS line
WITH split(line.row, '') AS row, line.y AS y
WITH [ix IN range (0, size(row)-1)| {x:ix, y:y-1, tile:row[ix]}] AS row
UNWIND row AS tile
WITH tile WHERE tile.tile = "#"
CREATE (elve:Position)
    SET elve:Elve
    SET elve = tile
MERGE (n:Position {x:elve.x, y:elve.y-1})
MERGE (ne:Position {x:elve.x+1, y:elve.y-1})
MERGE (e:Position {x:elve.x+1, y:elve.y})
MERGE (se:Position {x:elve.x+1, y:elve.y+1})
MERGE (s:Position {x:elve.x, y:elve.y+1})
MERGE (sw:Position {x:elve.x-1, y:elve.y+1})
MERGE (w:Position {x:elve.x-1, y:elve.y})
MERGE (nw:Position {x:elve.x-1, y:elve.y-1})
MERGE (elve)-[:N]->(n)
MERGE (elve)-[:NE]->(ne)
MERGE (elve)-[:E]->(e)
MERGE (elve)-[:SE]->(se)
MERGE (elve)-[:S]->(s)
MERGE (elve)-[:SW]->(sw)
MERGE (elve)-[:W]->(w)
MERGE (elve)-[:NW]->(nw);


/// CREATE DIR ORDER

CREATE
  (n:Direction:Init:Current {type:"N"}),
  (ne:Direction {type:"NE"}),
  (e:Direction {type:"E"}),
  (se:Direction {type:"SE"}),
  (s:Direction {type:"S"}),
  (sw:Direction {type:"SW"}),
  (w:Direction {type:"W"}),
  (nw:Direction {type:"NW"})
CREATE
  (n)-[:NEXT]->(s)-[:NEXT]->(w)-[:NEXT]->(e)-[:NEXT]->(n)
CREATE
  (n)-[:CW_NEXT]->(ne)-[:CW_NEXT]->(e)-[:CW_NEXT]->(se)-[:CW_NEXT]->
    (s)-[:CW_NEXT]->(sw)-[:CW_NEXT]->(w)-[:CW_NEXT]->(nw)-[:CW_NEXT]->(n);

MATCH (e:Elve)
WITH count(e) AS elves_num
CREATE (:Log {round:0, moved: elves_num});

// END OF SETUP


CALL apoc.periodic.commit('
CALL apoc.cypher.runMany(\'

MATCH (elve:Elve&!Processed)
WHERE NOT EXISTS {(elve)-->(:Elve)}
SET elve:Processed;

MATCH pdir=()-[:CW_NEXT]->(d:Direction:Current)-[:CW_NEXT]->()
WITH [dir IN nodes(pdir) | dir.type] AS dirs, d
MATCH (elve:Elve&!Processed)
WHERE NOT EXISTS {(elve)-[r WHERE type(r) IN dirs]->(e:Elve)}
SET elve:Processed
WITH elve, d
MATCH (elve)-[move WHERE type(move)=d.type]->(pos:Position)
CREATE (elve)-[:PROPOSED_MOVE]->(pos);

MATCH (dir:Direction:Current)-[:NEXT]->(next_dir:Direction)
REMOVE dir:Current
SET next_dir:Current;

//STEP 2

MATCH (pos:Position)
WHERE EXISTS {(dir:Direction&Current&Init)}
AND COUNT {(pos)<-[:PROPOSED_MOVE]-(:Elve)} > 1
SET pos:Unreachable;

MATCH (elve:Elve)-[:PROPOSED_MOVE]->(pos:Position&!Unreachable)
WHERE EXISTS {(dir:Direction&Current&Init)}
REMOVE elve:Elve
SET pos:Elve
SET pos:NewElve;


MATCH (l:Log)
WHERE EXISTS {(dir:Direction&Current&Init)}
AND NOT EXISTS {(l)-[:NEXT_LOG]->()}
MATCH (e:NewElve)
WITH l, count(e) AS elves_moved
CREATE (next_log:Log {round: l.round+1, moved: elves_moved})
CREATE (l)-[:NEXT_LOG]->(next_log)
RETURN count(*) AS limit;

MATCH ()-[r:PROPOSED_MOVE]->()
WHERE EXISTS {(dir:Direction&Current&Init)}
DELETE r;

MATCH (pos:Position&Processed)
WHERE EXISTS {(dir:Direction&Current&Init)}
REMOVE pos:Processed;

MATCH (pos:Position&Unreachable)
WHERE EXISTS {(dir:Direction&Current&Init)}
REMOVE pos:Unreachable;

MATCH (elve:Elve)
WHERE EXISTS {(dir:Direction&Current&Init)}
MERGE (n:Position {x:elve.x, y:elve.y-1})
MERGE (ne:Position {x:elve.x+1, y:elve.y-1})
MERGE (e:Position {x:elve.x+1, y:elve.y})
MERGE (se:Position {x:elve.x+1, y:elve.y+1})
MERGE (s:Position {x:elve.x, y:elve.y+1})
MERGE (sw:Position {x:elve.x-1, y:elve.y+1})
MERGE (w:Position {x:elve.x-1, y:elve.y})
MERGE (nw:Position {x:elve.x-1, y:elve.y-1})
MERGE (elve)-[:N]->(n)
MERGE (elve)-[:NE]->(ne)
MERGE (elve)-[:E]->(e)
MERGE (elve)-[:SE]->(se)
MERGE (elve)-[:S]->(s)
MERGE (elve)-[:SW]->(sw)
MERGE (elve)-[:W]->(w)
MERGE (elve)-[:NW]->(nw);

MATCH (e:Position&NewElve)
WITH count(e) AS newelves
WITH newelves,
CASE
  WHEN EXISTS {(dir:Direction&Current&Init)}
  THEN 0
  ELSE 1
END AS notzeroifroundnotover
RETURN newelves + notzeroifroundnotover AS limit;

MATCH (e:Position&NewElve)
REMOVE e:NewElve;

MATCH (d:Direction&Init&Current)-[:NEXT]->(next_dir:Direction)
REMOVE d:Init
REMOVE d:Current
SET next_dir:Init
SET next_dir:Current;
\', {})
YIELD result
WITH result
WHERE result.limit IS NOT null
WITH result.limit AS limit
RETURN limit',{});


//////////////////


///// RESULT

MATCH (l:Log)
WHERE NOT EXISTS {(l)-->()}
RETURN l.round + 1 AS `part 2`;
