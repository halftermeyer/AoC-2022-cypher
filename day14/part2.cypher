:param env => 'input';

MATCH (n) DETACH DELETE n;

CREATE CONSTRAINT scanpoint_x_y
IF NOT EXISTS
FOR (p:ScanPoint) REQUIRE (p.x, p.y) IS NODE KEY;

CREATE CONSTRAINT point_x_y
IF NOT EXISTS
FOR (p:Point) REQUIRE (p.x, p.y) IS NODE KEY;

// PARSE DATA
LOAD CSV FROM 'file:///'+$env+'.txt' AS lines FIELDTERMINATOR "\n"
UNWIND lines AS line
WITH line WHERE line IS NOT null
WITH split(line, " -> ") AS points
WITH [p IN points | [z IN split(p, ',') | toInteger(z)]] AS points
WITH [p IN points | {x:p[0], y:p[1]}] AS points
WITH [ix IN range(0, size(points)-2) |
    {src:points[ix], tgt:points[ix+1]}] AS rels
UNWIND rels AS rel
MERGE (src:ScanPoint {x:rel.src.x, y:rel.src.y})
MERGE (tgt:ScanPoint {x:rel.tgt.x, y:rel.tgt.y})
SET src:Point, tgt:Point
MERGE (src)-[:LINES_TO]->(tgt);

// CREATE GRID
MATCH (s:ScanPoint)
WITH max(s.x) AS max_x, min(s.x) AS min_x, max(s.y) AS max_y
UNWIND range (min_x-(max_y+1), max_x+(max_y+1)) AS x
UNWIND range (0, max_y+2) AS y
MERGE (:Point {x:x, y:y});

MATCH (p:Point)
WITH p.x AS x, p.y AS y, p
ORDER BY x, y
WITH x, collect(p) AS v_line
CALL apoc.nodes.link(v_line, "DOWN");

MATCH (p:Point)
WITH p.x AS x, p.y AS y, p
ORDER BY y, x
WITH y, collect(p) AS v_line
CALL apoc.nodes.link(v_line, "RIGHT");

// CREATE ROCK
CALL {
MATCH p=(sp1:Point)-[:DOWN*1..]->(sp2:Point)
WHERE EXISTS {(sp1)-[:LINES_TO]-(sp2)}
RETURN p
UNION
MATCH p=(sp1:Point)-[:RIGHT*1..]->(sp2:Point)
WHERE EXISTS {(sp1)-[:LINES_TO]-(sp2)}
RETURN p
}
FOREACH (p IN nodes(p) | SET p:Rock);

// CREATE POURING SOURCE
MATCH (p:Point {x:500,y:0})
SET p:PouringSource;

// ENDLESS VOID IS NOW ROCK
MATCH (p:Point)
WITH p.y AS y, collect(p) AS points
ORDER bY y DESC LIMIT 1
FOREACH (p IN points | SET p:Rock);

// SET FALLING ORDER
MATCH (src:Point&!Rock)-[:DOWN]->(tgt:Point&!Rock)
MERGE (src)-[:FALLS {priority_weight: 0}]->(tgt);
MATCH (src:Point&!Rock)-[:DOWN]->()<-[:RIGHT]-(tgt:Point&!Rock)
MERGE (src)-[:FALLS {priority_weight: 1}]->(tgt);
MATCH (src:Point&!Rock)-[:DOWN]->()-[:RIGHT]->(tgt:Point&!Rock)
MERGE (src)-[:FALLS {priority_weight: 2}]->(tgt);


//////////// COMPUTE POURING SOURCE TRANSITIVE FALL CLOSURE /////////////////

MATCH (p:PouringSource)
SET p:Rest;
CALL apoc.periodic.commit('
MATCH (sand:Rest)-[:FALLS]->(p:Point&!Rest)
WITH DISTINCT p
SET p:Rest
WITH count(p) AS limit
RETURN limit',{});

MATCH (n:Rest) RETURN count(n) AS `part 2`;
