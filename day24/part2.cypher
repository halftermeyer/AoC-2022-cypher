CREATE (init:Init)
WITH init
MATCH (p:Position {y: 0})
MERGE (p)-[:MOVES]->(init);

CALL gds.graph.drop('space_time_map', false);
CALL gds.graph.project(
    'space_time_map',
    ['Position','Target','Init'],
    'MOVES',
    {}
);

///

MATCH (n:Result) DETACH DELETE n;
MATCH (n:Stop_1) REMOVE n:Stop_1;
MATCH (n:Stop_2) REMOVE n:Stop_2;
MATCH (n:Stop_3) REMOVE n:Stop_3;


MATCH (source:Position {y: 0, t:0}), (target:Target)
CALL gds.shortestPath.dijkstra.stream('space_time_map', {
    sourceNode: source,
    targetNode: target
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
WITH
    nodes(path) as path
ORDER BY index
WITH size(path) - 2 AS len, path[-2] AS stop_1
CREATE (:Result {len:len})
WITH stop_1
SET stop_1:Stop_1;

MATCH (source:Stop_1), (target:Init)
CALL gds.shortestPath.dijkstra.stream('space_time_map', {
    sourceNode: source,
    targetNode: target
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
WITH
    nodes(path) as path
ORDER BY index
WITH size(path) - 2 AS len, path[-2] AS stop_2
CREATE (:Result {len:len})
WITH stop_2
SET stop_2:Stop_2;

MATCH (source:Stop_2), (target:Target)
CALL gds.shortestPath.dijkstra.stream('space_time_map', {
    sourceNode: source,
    targetNode: target
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
WITH
    nodes(path) as path
ORDER BY index
WITH size(path) - 2 AS len, path[-2] AS stop_3
CREATE (:Result {len:len})
WITH stop_3
SET stop_3:Stop_3;

MATCH (r:Result)
RETURN sum (r.len) AS `part 2`
