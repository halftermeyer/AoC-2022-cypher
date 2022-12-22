:param env => 'test';

MATCH (n) DETACH DELETE n;

// PARSE MAP

CREATE CONSTRAINT tile_x_y
IF NOT EXISTS
FOR (t:Tile) REQUIRE (t.x, t.y) IS NODE KEY;

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH collect(line)[..-1] AS lines
WITH [ix IN range (0, size(lines)-1)| {y:ix+1, row:lines[ix][0]}] AS lines
UNWIND lines AS line
WITH split(line.row, '') AS row, line.y AS y
WITH [ix IN range (0, size(row)-1)| {x:ix+1, y:y, tile:row[ix]}] AS row
UNWIND row AS tile
WITH tile WHERE tile.tile <> " "
CREATE (t:Tile)
    SET t = tile;

MATCH (t:Tile)
WITH DISTINCT t.x AS x
MATCH (t:Tile WHERE t.x = x)
WITH x, t, t.y AS y
ORDER BY y
WITH x, collect(t) AS col
CALL apoc.nodes.link (col, "DOWN")
WITH col[0] AS first, col[-1] AS last
CREATE (last)-[:DOWN]->(first);

MATCH (t:Tile)
WITH DISTINCT t.y AS y
MATCH (t:Tile WHERE t.y = y)
WITH y, t, t.x AS x
ORDER BY x
WITH y, collect(t) AS row
CALL apoc.nodes.link (row, "RIGHT")
WITH row[0] AS first, row[-1] AS last
CREATE (last)-[:RIGHT]->(first);

MATCH (t:Tile {y:1})
WITH t.x AS x, t
ORDER BY x LIMIT 1
SET t:Init:Current;

// PARSE PATH

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH collect(line)[-1][0] AS line
WITH line
WITH apoc.text.regexGroups(
  line,
  '(\\d+)(\\w{0,1})'
) AS parsedLine
WITH [x IN parsedLine | [x[1], x[2]]] AS parsedLine
WITH apoc.coll.flatten(parsedLine) AS parsedLine
WITH [x IN parsedLine WHERE x <> ""] AS parsedLine
WITH [ix IN range(0, size(parsedLine)-1)|
    {
        ix: ix,
        val: parsedLine[ix],
        type:CASE
                WHEN parsedLine[ix] IN ['R','L']
                THEN "rotate"
                ELSE "walk"
            END
    }] AS parsedLine
UNWIND parsedLine AS step
CREATE (s:Step)
    SET s = step;

MATCH (s:Step)
WITH s.ix AS ix, s
ORDER BY ix
WITH collect(s) AS steps
CALL apoc.nodes.link(steps, "NEXT");

MATCH (s:Step {ix:0})
SET s:Init:Current;

CREATE
  (r:Direction:init:Current {sym:">", type:"RIGHT>", val:0}),
  (d:Direction {sym:"v", type:"DOWN>", val:1}),
  (l:Direction {sym:"<", type:"<RIGHT", val:2}),
  (u:Direction {sym:"^", type:"<DOWN", val:3})
CREATE (r)-[:ROTATE {val:'R'}]->(d)-[:ROTATE {val:'R'}]->
  (l)-[:ROTATE {val:'R'}]->(u)-[:ROTATE {val:'R'}]->(r)
CREATE (r)-[:ROTATE {val:'L'}]->(u)-[:ROTATE {val:'L'}]->
  (l)-[:ROTATE {val:'L'}]->(d)-[:ROTATE {val:'L'}]->(r);

MATCH (t:Tile WHERE t.tile = "#")
DETACH DELETE t;

/// RUN
CALL apoc.periodic.commit('
CALL apoc.cypher.runMany(\'
  MATCH (pos:Tile&Current), (step:Step&Current), (dir:Direction&Current)
  WHERE step.type = "rotate"
  MATCH (dir)-[:ROTATE {val:step.val}]->(new_dir:Direction)
  REMOVE dir:Current
  SET new_dir:Current;

  MATCH (pos:Tile&Current), (step:Step&Current), (dir:Direction&Current)
  WHERE step.type = "walk"
  CALL apoc.path.expandConfig(pos,
      {
        relationshipFilter: dir.type,
        minLevel: 0,
        maxLevel: step.val,
        uniqueness:"NONE"
        }
      )
  YIELD path
  WITH *, nodes(path)[-1] AS target, size(nodes(path)) AS len
  ORDER BY len DESC limit 1
  REMOVE pos:Current
  SET target:Current;

  MATCH (step:Step&Current)-[:NEXT]->(next_step:Step)
  REMOVE step:Current
  SET next_step:Current
  RETURN count(*) AS limit;
\', {})
YIELD result
WITH result
WHERE result.limit IS NOT null
WITH result.limit AS limit
RETURN limit',{});

MATCH (pos:Tile&Current),(dir:Direction&Current)
RETURN 1000 * pos.y + 4 * pos.x + dir.val;
