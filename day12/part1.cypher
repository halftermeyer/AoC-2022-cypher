//input or test
:param env => 'input';

MATCH (n) DETACH DELETE n;

CREATE INDEX square_lat_lon
IF NOT EXISTS
FOR (sq:Square) ON (sq.lat, sq.lon);

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH split(line[0], "") AS heights,  linenumber() - 1 AS lat
WITH lat, [lon IN range(0, size(heights)- 1) | {height:heights[lon], lon:lon, lat:lat}] AS squares
UNWIND squares AS square
CREATE (sq:Square) SET sq = square;

MATCH (anySq:Square)
WITH DISTINCT anySq.lat AS max_lat
ORDER BY max_lat DESC LIMIT 1
UNWIND range(0,max_lat) AS lat
MATCH (sq:Square WHERE sq.lat = lat)
WITH lat, sq, sq.lon AS lon
ORDER BY lon
WITH lat, collect(sq) AS squares
CALL apoc.nodes.link(squares, "EAST");

MATCH (anySq:Square)
WITH DISTINCT anySq.lon AS max_lon
ORDER BY max_lon DESC LIMIT 1
UNWIND range(0,max_lon) AS lon
MATCH (sq:Square WHERE sq.lon = lon)
WITH lon, sq, sq.lat AS lat
ORDER BY lon
WITH lon, collect(sq) AS squares
CALL apoc.nodes.link(squares, "SOUTH");

MATCH (sq:Square {height: 'S'})
SET sq:Initial
SET sq.height = 'a';
MATCH (sq:Square {height: 'E'})
SET sq:Destination
SET sq.height = 'z';
MATCH (sq:Square)
SET sq.elevation = apoc.text.indexOf(
  "abcdefghijklmnopqrstuvwxyz", sq.height);

MATCH (sq_from:Square)-[r:EAST|SOUTH]-(sq_to)
WHERE sq_to.elevation <= sq_from.elevation + 1
MERGE (sq_from)-[:POSSIBLE_STEP
  {v_gain: sq_to.elevation - sq_from.elevation}]->(sq_to);

CALL gds.graph.drop('height_map', false);

CALL gds.graph.project(
    'height_map',
    'Square',
    'POSSIBLE_STEP',
    {}
);

MATCH (source:Square&Initial), (target:Square&Destination)
CALL gds.shortestPath.dijkstra.stream('height_map', {
    sourceNode: source,
    targetNode: target
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN toInteger(totalCost) AS `part 1`;
