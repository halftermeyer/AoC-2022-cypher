:param env => 'test';

MATCH (n) DETACH DELETE n;

CREATE INDEX tree_row_col
IF NOT EXISTS
FOR (t:Tree) ON (t.row, t.col);

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH linenumber()-1 AS row, [h IN split(line[0], '')|toInteger(h)] AS tHeights
WITH row, [ix IN range(0, size(tHeights)-1)|{row: row, col: ix, height: tHeights[ix]}] AS tHeights
FOREACH (th IN tHeights | CREATE (t:Tree) SET t=th);

MATCH (anyTree:Tree)
WITH DISTINCT anyTree.row AS row
ORDER BY row
MATCH (t:Tree {row: row})
WITH row, t ORDER BY row, t.col
WITH row, collect(t) AS rowTrees
CALL apoc.nodes.link(rowTrees, "EAST_NEIGHBOR");

MATCH (anyTree:Tree)
WITH DISTINCT anyTree.col AS col
ORDER BY col
MATCH (t:Tree {col: col})
WITH col, t ORDER BY col, t.row
WITH col, collect(t) AS colTrees
CALL apoc.nodes.link(colTrees, "SOUTH_NEIGHBOR");

MATCH (t:Tree)
WHERE NOT EXISTS {(t)-[:EAST_NEIGHBOR]->()}
SET t.east_horizon_height = -1;

MATCH (t:Tree)
WHERE NOT EXISTS {(t)<-[:EAST_NEIGHBOR]-()}
SET t.west_horizon_height = -1;

MATCH (t:Tree)
WHERE NOT EXISTS {(t)-[:SOUTH_NEIGHBOR]->()}
SET t.south_horizon_height = -1;

MATCH (t:Tree)
WHERE NOT EXISTS {(t)<-[:SOUTH_NEIGHBOR]-()}
SET t.north_horizon_height = -1;

CALL apoc.periodic.commit("
MATCH (t:Tree)-[:EAST_NEIGHBOR]->(n:Tree)
WHERE t.east_horizon_height IS null
AND n.east_horizon_height IS NOT null
SET t.east_horizon_height = apoc.coll.max([n.height, n.east_horizon_height])
WITH count(t) AS limit
RETURN limit", {});

CALL apoc.periodic.commit("
MATCH (t:Tree)<-[:EAST_NEIGHBOR]-(n:Tree)
WHERE t.west_horizon_height IS null
AND n.west_horizon_height IS NOT null
SET t.west_horizon_height = apoc.coll.max([n.height, n.west_horizon_height])
WITH count(t) AS limit
RETURN limit", {});

CALL apoc.periodic.commit("
MATCH (t:Tree)-[:SOUTH_NEIGHBOR]->(n:Tree)
WHERE t.south_horizon_height IS null
AND n.south_horizon_height IS NOT null
SET t.south_horizon_height = apoc.coll.max([n.height, n.south_horizon_height])
WITH count(t) AS limit
RETURN limit", {});

CALL apoc.periodic.commit(
"
MATCH (t:Tree)<-[:SOUTH_NEIGHBOR]-(n:Tree)
WHERE t.north_horizon_height IS null
AND n.north_horizon_height IS NOT null
SET t.north_horizon_height = apoc.coll.max([n.height, n.north_horizon_height])
WITH count(t) AS limit
RETURN limit",
{});

MATCH (visibleTree:Tree)
WHERE any(h IN
    [
        visibleTree.north_horizon_height,
        visibleTree.south_horizon_height,
        visibleTree.east_horizon_height,
        visibleTree.west_horizon_height
    ]
    WHERE visibleTree.height > h)
RETURN count(visibleTree) AS `part 1`;
