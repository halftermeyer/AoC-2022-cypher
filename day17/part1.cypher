:param env => 'test';
:param shapes => 'shapes';
:param shape_nb => 2022;

CREATE CONSTRAINT pix_x_y
IF NOT EXISTS
FOR (p:Pix) REQUIRE (p.x, p.y) IS NODE KEY;

MATCH (n) DETACH DELETE n;

// parse
LOAD CSV FROM 'file:///'+$env+'.txt' AS line FIELDTERMINATOR "\n"
WITH line[0] AS jet_pattern
UNWIND split(jet_pattern, '') AS dir
CREATE (jet:Jet {direction: dir})
WITH collect (jet) AS jet_pattern
CALL apoc.nodes.link(jet_pattern, "NEXT");

MATCH (first:Jet WHERE NOT EXISTS {()-[:NEXT]->(first)}),
(last:Jet WHERE NOT EXISTS {(last)-[:NEXT]->()})
SET first:Current:First
CREATE (last)-[:NEXT]->(first);

// parse rocks
LOAD CSV FROM 'file:///'+$shapes+'.txt' AS lines FIELDTERMINATOR "\n"
CALL apoc.coll.split(lines, null) YIELD value
WITH reverse(value) AS shape
UNWIND range (0, size(shape)-1) AS delta_y
WITH *, split(shape[delta_y], '') AS row
UNWIND range(0, size(row)-1) AS delta_x
WITH *, delta_x, row[delta_x] AS sym
WHERE sym = "#"
WITH shape, {delta_x:delta_x, delta_y:delta_y} AS rock_pix
WITH shape, collect(rock_pix) AS rock_pixs
WITH shape, apoc.convert.toJson(rock_pixs) AS json_rock_pix
CREATE (r:RockType {sprite: json_rock_pix})
WITH collect(r) AS rocks
CALL apoc.nodes.link(rocks, "NEXT");

MATCH (first:RockType WHERE NOT EXISTS {()-[:NEXT]->(first)}),
(last:RockType WHERE NOT EXISTS {(last)-[:NEXT]->()})
SET first:Current:First
CREATE (last)-[:NEXT]->(first);

// CREATE GRID
UNWIND range(0,8) AS x
UNWIND range(0, 3 * $shape_nb) AS y
CREATE (p:Pix {x:x, y: y});

MATCH (p:Pix)
WITH DISTINCT p.x AS x
MATCH (p:Pix WHERE p.x = x)
WITH x, p, p.y AS y
ORDER BY y DESC
WITH x, collect(p) AS col
CALL apoc.nodes.link (col, "DOWN");

MATCH (p:Pix)
WITH DISTINCT p.y AS y
MATCH (p:Pix WHERE p.y = y)
WITH y, p, p.x AS x
ORDER BY x
WITH y, collect(p) AS row
CALL apoc.nodes.link (row, "RIGHT");

MATCH (p:Pix WHERE p.y = 0)
SET p:Rest:Bottom;
MATCH (p:Pix WHERE p.x IN [0,8])
SET p:Rest:Wall

///////// END OF SETUP ///////
