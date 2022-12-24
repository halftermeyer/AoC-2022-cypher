:param env => 'test';

CALL apoc.periodic.iterate("MATCH (n) RETURN n",
"DETACH DELETE n",{batchSize:10000});

// PARSE MAP

CREATE CONSTRAINT Position_x_y_t
IF NOT EXISTS
FOR (p:Position) REQUIRE (p.x, p.y, p.t) IS NODE KEY;

CALL apoc.periodic.iterate("
LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH size(split(line[0], '')) AS width, line
WITH collect(width) AS ws
WITH size(ws) AS height, ws[0] AS width
WITH height, width, (height-2)*(width-2) AS duration
CREATE (:Dimension {width:width, height:height, duration:duration})
WITH height, width, duration
UNWIND range(0, duration-1) AS t
UNWIND range(0, width-1) AS x
UNWIND range(0, height-1) AS y
WITH t, x, y
RETURN t, x, y
","
CREATE (p:Position {t:t, x:x, y:y, tile: '.'})",
{batchSize:1000, parallel:true, params:{env:$env}});


CALL apoc.periodic.iterate("
LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH collect(line) AS lines
WITH [ix IN range (0, size(lines)-1)| {y:ix+1, row:lines[ix][0]}] AS lines
UNWIND lines AS line
WITH split(line.row, '') AS row, line.y AS y
WITH [ix IN range (0, size(row)-1)| {x:ix, y:y-1, tile:row[ix]}] AS row
UNWIND row AS tile
// CASE #
WITH tile
WHERE tile.tile = '#'
RETURN tile
","
MATCH (p:Position {x:tile.x, y:tile.y})
    SET p.tile = '#'",
{batchSize:1, parallel:false, params:{env:$env}});


CALL apoc.periodic.iterate("
MATCH (dim:Dimension)
LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH dim, collect(line) AS lines
WITH dim, [ix IN range (0, size(lines)-1)| {y:ix+1, row:lines[ix][0]}] AS lines
UNWIND lines AS line
WITH dim, split(line.row, '') AS row, line.y AS y
WITH dim, [ix IN range (0, size(row)-1)| {x:ix, y:y-1, tile:row[ix]}] AS row
UNWIND row AS tile
// CASE >
WITH dim, tile
WHERE tile.tile = '>'
RETURN dim, tile","
MATCH (p:Position WHERE (p.x - 1) % (dim.width-2) = (tile.x + p.t -1) % (dim.width-2)
        AND p.y=tile.y)
SET p.tile = '#'",
{batchSize:1, parallel:false, params:{env:$env}});

CALL apoc.periodic.iterate("
MATCH (dim:Dimension)
LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH dim, collect(line) AS lines
WITH dim, [ix IN range (0, size(lines)-1)| {y:ix+1, row:lines[ix][0]}] AS lines
UNWIND lines AS line
WITH dim, split(line.row, '') AS row, line.y AS y
WITH dim, [ix IN range (0, size(row)-1)| {x:ix, y:y-1, tile:row[ix]}] AS row
UNWIND row AS tile
// CASE <
WITH dim, tile
WHERE tile.tile = '<'
RETURN dim, tile","
MATCH (p:Position WHERE (p.x - 1) % (dim.width-2)
  = ((tile.x - p.t -1) % (dim.width-2) + dim.width-2) % (dim.width-2)
        AND p.y=tile.y)
SET p.tile = '#'",
{batchSize:1, parallel:false, params:{env:$env}});

CALL apoc.periodic.iterate("
MATCH (dim:Dimension)
LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH dim, collect(line) AS lines
WITH dim, [ix IN range (0, size(lines)-1)| {y:ix+1, row:lines[ix][0]}] AS lines
UNWIND lines AS line
WITH dim, split(line.row, '') AS row, line.y AS y
WITH dim, [ix IN range (0, size(row)-1)| {x:ix, y:y-1, tile:row[ix]}] AS row
UNWIND row AS tile
// CASE v
WITH dim, tile
WHERE tile.tile = 'v'
RETURN dim, tile","
MATCH (p:Position WHERE p.x = tile.x
        AND (p.y - 1) % (dim.height - 2) = (tile.y + p.t - 1) % (dim.height - 2))
SET p.tile = '#'",
{batchSize:1, parallel:false, params:{env:$env}});

CALL apoc.periodic.iterate("
MATCH (dim:Dimension)
LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH dim, collect(line) AS lines
WITH dim, [ix IN range (0, size(lines)-1)| {y:ix+1, row:lines[ix][0]}] AS lines
UNWIND lines AS line
WITH dim, split(line.row, '') AS row, line.y AS y
WITH dim, [ix IN range (0, size(row)-1)| {x:ix, y:y-1, tile:row[ix]}] AS row
UNWIND row AS tile
// CASE ^
WITH dim, tile
WHERE tile.tile = '^'
RETURN dim, tile","
MATCH (p:Position WHERE p.x = tile.x
        AND (p.y - 1) % (dim.height - 2)
        = ((tile.y - p.t - 1) % (dim.height - 2) + dim.height-2) % (dim.height-2))
    SET p.tile = '#'",
{batchSize:1, parallel:false, params:{env:$env}});

// CALL apoc.periodic.iterate("
// MATCH (dim:Dimension)
// MATCH (src:Position)
// MATCH (tgt:Position)
// WHERE tgt.t = (src.t+1) % (dim.duration)
// AND abs(tgt.x-src.x) + abs(tgt.y-src.y) <= 1
// RETURN src, tgt",
// "MERGE (src)-[:MOVES]->(tgt)",
// {batchSize:1000});

CALL apoc.periodic.iterate("
MATCH (dim:Dimension)
MATCH (src:Position)
UNWIND [{x:0, y:-1}, {x:0, y:0}, {x:0, y:1}, {x:-1, y:0}, {x:1, y:0}] AS offset
MATCH (tgt:Position {t:(src.t+1) % (dim.duration),
  x:src.x + offset.x,
  y:src.y + offset.y})
RETURN src, tgt",
"MERGE (src)-[:MOVES]->(tgt)",
{batchSize:1000});


CALL apoc.periodic.iterate("
MATCH (p:Position WHERE p.tile <> '.')
RETURN p",
"DETACH DELETE p",
{batchSize:1000});

MATCH (dim:Dimension)
CREATE (target:Target)
WITH target, dim
MATCH (p:Position {y: dim.height-1})
MERGE (p)-[:MOVES]->(target);


CALL gds.graph.drop('space_time_map', false);
CALL gds.graph.project(
    'space_time_map',
    ['Position','Target'],
    'MOVES',
    {}
);

MATCH (source:Position {y: 0, t:0}), (target:Target)
CALL gds.shortestPath.dijkstra.stream('space_time_map', {
    sourceNode: source,
    targetNode: target
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
WITH
    index,
    gds.util.asNode(sourceNode).name AS sourceNodeName,
    gds.util.asNode(targetNode).name AS targetNodeName,
    totalCost,
    [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
    costs,
    nodes(path) as path
ORDER BY index
RETURN path[-2].t AS `part 1`;
