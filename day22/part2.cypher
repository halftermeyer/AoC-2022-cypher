:param env => 'input';

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
WITH t, (t.x-1)/50 + 1 AS X
SET t.X = X;
MATCH (t:Tile)
WITH t, (t.y-1)/50 + 1 AS Y
SET t.Y = Y;

MATCH (t:Tile)
WITH DISTINCT t.x AS x, t.Y AS Y
MATCH (t:Tile WHERE t.x = x AND t.Y = Y)
WITH x, t, t.y AS y, Y
ORDER BY y
WITH x, Y, collect(t) AS col
CALL apoc.nodes.link (col, "DOWN");

MATCH (t:Tile)
WITH DISTINCT t.y AS y, t.X AS X
MATCH (t:Tile WHERE t.y = y AND t.X = X)
WITH y, t, t.x AS x, X
ORDER BY x
WITH y, X, collect(t) AS col
CALL apoc.nodes.link (col, "RIGHT");

MATCH (t:Tile {y:1})
WITH t.x AS x, t
ORDER BY x LIMIT 1
SET t:Init:Current;

// Build the Cube
// green rels
UNWIND [{X:2,Y:1}, {X:2,Y:2}, {X:2,Y:3}] AS box
MATCH (a:Tile)<-[:DOWN]-(b:Tile)
WHERE all(x IN [a, b] WHERE x.X = box.X
AND x.Y = box.Y)
CREATE (a)-[:GREEN]->(b);

UNWIND [{X:1,Y:4}] AS box
MATCH (a:Tile)-[:RIGHT]->(b:Tile)
WHERE all(x IN [a, b] WHERE x.X = box.X
AND x.Y = box.Y)
CREATE (a)-[:GREEN]->(b);

// blue rels
UNWIND [{X:1,Y:3}, {X:1,Y:4}, {X:3,Y:1}] AS box
MATCH (a:Tile)<-[:DOWN]-(b:Tile)
WHERE all(x IN [a, b] WHERE x.X = box.X
AND x.Y = box.Y)
CREATE (a)-[:BLUE]->(b);

UNWIND [{X:2,Y:2}] AS box
MATCH (a:Tile)-[:RIGHT]->(b:Tile)
WHERE all(x IN [a, b] WHERE x.X = box.X
AND x.Y = box.Y)
CREATE (a)-[:BLUE]->(b);

// black rels

UNWIND [{X:2,Y:1}, {X:3,Y:1}] AS box
MATCH (a:Tile)-[:RIGHT]->(b:Tile)
WHERE all(x IN [a, b] WHERE x.X = box.X
AND x.Y = box.Y)
CREATE (a)-[:BLACK]->(b);

UNWIND [{X:1,Y:3}, {X:2,Y:3}] AS box
MATCH (a:Tile)<-[:RIGHT]-(b:Tile)
WHERE all(x IN [a, b] WHERE x.X = box.X
AND x.Y = box.Y)
CREATE (a)-[:BLACK]->(b);

//////CUBE EDGES

// GREEN EDGES
WITH {X:2, Y:2} AS src, {X:2, Y:1} AS tgt
MATCH (t_src:Tile {X:src.X, Y:src.Y})
WHERE NOT EXISTS {(t_src)-[:GREEN]->()}
WITH tgt, t_src, t_src.x AS x, t_src.y AS y
ORDER BY x ASC
WITH tgt, collect(t_src) AS t_src_s
MATCH (t_tgt:Tile {X:tgt.X, Y:tgt.Y})
WHERE NOT EXISTS {()-[:GREEN]->(t_tgt)}
WITH t_src_s, t_tgt, t_tgt.x AS x, t_tgt.y AS y
ORDER BY x ASC
WITH t_src_s, collect(t_tgt) AS t_tgt_s
UNWIND range(0, size(t_src_s)-1) AS ix
WITH ix, t_src_s[ix] AS s, t_tgt_s[ix] AS t
CREATE (s)-[:GREEN]->(t);

WITH {X:2, Y:3} AS src, {X:2, Y:2} AS tgt
MATCH (t_src:Tile {X:src.X, Y:src.Y})
WHERE NOT EXISTS {(t_src)-[:GREEN]->()}
WITH tgt, t_src, t_src.x AS x, t_src.y AS y
ORDER BY x ASC
WITH tgt, collect(t_src) AS t_src_s
MATCH (t_tgt:Tile {X:tgt.X, Y:tgt.Y})
WHERE NOT EXISTS {()-[:GREEN]->(t_tgt)}
WITH t_src_s, t_tgt, t_tgt.x AS x, t_tgt.y AS y
ORDER BY x ASC
WITH t_src_s, collect(t_tgt) AS t_tgt_s
UNWIND range(0, size(t_src_s)-1) AS ix
WITH ix, t_src_s[ix] AS s, t_tgt_s[ix] AS t
CREATE (s)-[:GREEN]->(t);

WITH {X:1, Y:4} AS src, {X:2, Y:3} AS tgt
MATCH (t_src:Tile {X:src.X, Y:src.Y})
WHERE NOT EXISTS {(t_src)-[:GREEN]->()}
WITH tgt, t_src, t_src.x AS x, t_src.y AS y
ORDER BY y ASC
WITH tgt, collect(t_src) AS t_src_s
MATCH (t_tgt:Tile {X:tgt.X, Y:tgt.Y})
WHERE NOT EXISTS {()-[:GREEN]->(t_tgt)}
WITH t_src_s, t_tgt, t_tgt.x AS x, t_tgt.y AS y
ORDER BY x ASC
WITH t_src_s, collect(t_tgt) AS t_tgt_s
UNWIND range(0, size(t_src_s)-1) AS ix
WITH ix, t_src_s[ix] AS s, t_tgt_s[ix] AS t
CREATE (s)-[:GREEN]->(t);

WITH {X:2, Y:1} AS src, {X:1, Y:4} AS tgt
MATCH (t_src:Tile {X:src.X, Y:src.Y})
WHERE NOT EXISTS {(t_src)-[:GREEN]->()}
WITH tgt, t_src, t_src.x AS x, t_src.y AS y
ORDER BY x ASC
WITH tgt, collect(t_src) AS t_src_s
MATCH (t_tgt:Tile {X:tgt.X, Y:tgt.Y})
WHERE NOT EXISTS {()-[:GREEN]->(t_tgt)}
WITH t_src_s, t_tgt, t_tgt.x AS x, t_tgt.y AS y
ORDER BY y ASC
WITH t_src_s, collect(t_tgt) AS t_tgt_s
UNWIND range(0, size(t_src_s)-1) AS ix
WITH ix, t_src_s[ix] AS s, t_tgt_s[ix] AS t
CREATE (s)-[:GREEN]->(t);

// BLUE EDGES

WITH {X:1, Y:4} AS src, {X:1, Y:3} AS tgt
MATCH (t_src:Tile {X:src.X, Y:src.Y})
WHERE NOT EXISTS {(t_src)-[:BLUE]->()}
WITH tgt, t_src, t_src.x AS x, t_src.y AS y
ORDER BY x ASC
WITH tgt, collect(t_src) AS t_src_s
MATCH (t_tgt:Tile {X:tgt.X, Y:tgt.Y})
WHERE NOT EXISTS {()-[:BLUE]->(t_tgt)}
WITH t_src_s, t_tgt, t_tgt.x AS x, t_tgt.y AS y
ORDER BY x ASC
WITH t_src_s, collect(t_tgt) AS t_tgt_s
UNWIND range(0, size(t_src_s)-1) AS ix
WITH ix, t_src_s[ix] AS s, t_tgt_s[ix] AS t
CREATE (s)-[:BLUE]->(t);

WITH {X:1, Y:3} AS src, {X:2, Y:2} AS tgt
MATCH (t_src:Tile {X:src.X, Y:src.Y})
WHERE NOT EXISTS {(t_src)-[:BLUE]->()}
WITH tgt, t_src, t_src.x AS x, t_src.y AS y
ORDER BY x ASC
WITH tgt, collect(t_src) AS t_src_s
MATCH (t_tgt:Tile {X:tgt.X, Y:tgt.Y})
WHERE NOT EXISTS {()-[:BLUE]->(t_tgt)}
WITH t_src_s, t_tgt, t_tgt.x AS x, t_tgt.y AS y
ORDER BY y ASC
WITH t_src_s, collect(t_tgt) AS t_tgt_s
UNWIND range(0, size(t_src_s)-1) AS ix
WITH ix, t_src_s[ix] AS s, t_tgt_s[ix] AS t
CREATE (s)-[:BLUE]->(t);

WITH {X:2, Y:2} AS src, {X:3, Y:1} AS tgt
MATCH (t_src:Tile {X:src.X, Y:src.Y})
WHERE NOT EXISTS {(t_src)-[:BLUE]->()}
WITH tgt, t_src, t_src.x AS x, t_src.y AS y
ORDER BY y ASC
WITH tgt, collect(t_src) AS t_src_s
MATCH (t_tgt:Tile {X:tgt.X, Y:tgt.Y})
WHERE NOT EXISTS {()-[:BLUE]->(t_tgt)}
WITH t_src_s, t_tgt, t_tgt.x AS x, t_tgt.y AS y
ORDER BY x ASC
WITH t_src_s, collect(t_tgt) AS t_tgt_s
UNWIND range(0, size(t_src_s)-1) AS ix
WITH ix, t_src_s[ix] AS s, t_tgt_s[ix] AS t
CREATE (s)-[:BLUE]->(t);

WITH {X:3, Y:1} AS src, {X:1, Y:4} AS tgt
MATCH (t_src:Tile {X:src.X, Y:src.Y})
WHERE NOT EXISTS {(t_src)-[:BLUE]->()}
WITH tgt, t_src, t_src.x AS x, t_src.y AS y
ORDER BY x ASC
WITH tgt, collect(t_src) AS t_src_s
MATCH (t_tgt:Tile {X:tgt.X, Y:tgt.Y})
WHERE NOT EXISTS {()-[:BLUE]->(t_tgt)}
WITH t_src_s, t_tgt, t_tgt.x AS x, t_tgt.y AS y
ORDER BY x ASC
WITH t_src_s, collect(t_tgt) AS t_tgt_s
UNWIND range(0, size(t_src_s)-1) AS ix
WITH ix, t_src_s[ix] AS s, t_tgt_s[ix] AS t
CREATE (s)-[:BLUE]->(t);

// BLACK EDGES

WITH {X:2, Y:1} AS src, {X:3, Y:1} AS tgt
MATCH (t_src:Tile {X:src.X, Y:src.Y})
WHERE NOT EXISTS {(t_src)-[:BLACK]->()}
WITH tgt, t_src, t_src.x AS x, t_src.y AS y
ORDER BY y ASC
WITH tgt, collect(t_src) AS t_src_s
MATCH (t_tgt:Tile {X:tgt.X, Y:tgt.Y})
WHERE NOT EXISTS {()-[:BLACK]->(t_tgt)}
WITH t_src_s, t_tgt, t_tgt.x AS x, t_tgt.y AS y
ORDER BY y ASC
WITH t_src_s, collect(t_tgt) AS t_tgt_s
UNWIND range(0, size(t_src_s)-1) AS ix
WITH ix, t_src_s[ix] AS s, t_tgt_s[ix] AS t
CREATE (s)-[:BLACK]->(t);

WITH {X:2, Y:3} AS src, {X:1, Y:3} AS tgt
MATCH (t_src:Tile {X:src.X, Y:src.Y})
WHERE NOT EXISTS {(t_src)-[:BLACK]->()}
WITH tgt, t_src, t_src.x AS x, t_src.y AS y
ORDER BY y ASC
WITH tgt, collect(t_src) AS t_src_s
MATCH (t_tgt:Tile {X:tgt.X, Y:tgt.Y})
WHERE NOT EXISTS {()-[:BLACK]->(t_tgt)}
WITH t_src_s, t_tgt, t_tgt.x AS x, t_tgt.y AS y
ORDER BY y ASC
WITH t_src_s, collect(t_tgt) AS t_tgt_s
UNWIND range(0, size(t_src_s)-1) AS ix
WITH ix, t_src_s[ix] AS s, t_tgt_s[ix] AS t
CREATE (s)-[:BLACK]->(t);

WITH {X:3, Y:1} AS src, {X:2, Y:3} AS tgt
MATCH (t_src:Tile {X:src.X, Y:src.Y})
WHERE NOT EXISTS {(t_src)-[:BLACK]->()}
WITH tgt, t_src, t_src.x AS x, t_src.y AS y
ORDER BY y ASC
WITH tgt, collect(t_src) AS t_src_s
MATCH (t_tgt:Tile {X:tgt.X, Y:tgt.Y})
WHERE NOT EXISTS {()-[:BLACK]->(t_tgt)}
WITH t_src_s, t_tgt, t_tgt.x AS x, t_tgt.y AS y
ORDER BY y DESC
WITH t_src_s, collect(t_tgt) AS t_tgt_s
UNWIND range(0, size(t_src_s)-1) AS ix
WITH ix, t_src_s[ix] AS s, t_tgt_s[ix] AS t
CREATE (s)-[:BLACK]->(t);

WITH {X:1, Y:3} AS src, {X:2, Y:1} AS tgt
MATCH (t_src:Tile {X:src.X, Y:src.Y})
WHERE NOT EXISTS {(t_src)-[:BLACK]->()}
WITH tgt, t_src, t_src.x AS x, t_src.y AS y
ORDER BY y ASC
WITH tgt, collect(t_src) AS t_src_s
MATCH (t_tgt:Tile {X:tgt.X, Y:tgt.Y})
WHERE NOT EXISTS {()-[:BLACK]->(t_tgt)}
WITH t_src_s, t_tgt, t_tgt.x AS x, t_tgt.y AS y
ORDER BY y DESC
WITH t_src_s, collect(t_tgt) AS t_tgt_s
UNWIND range(0, size(t_src_s)-1) AS ix
WITH ix, t_src_s[ix] AS s, t_tgt_s[ix] AS t
CREATE (s)-[:BLACK]->(t);


//// END CUBE

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
        val: CASE
                WHEN parsedLine[ix] IN ['R','L']
                THEN parsedLine[ix]
                ELSE toInteger(parsedLine[ix])
            END,
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
  (r:Direction {sym:">", type:"RIGHT", natural:true, val:0}),
  (d:Direction {sym:"v", type:"DOWN", natural:true, val:1}),
  (l:Direction {sym:"<", type:"RIGHT", natural:false, val:2}),
  (u:Direction {sym:"^", type:"DOWN", natural:false, val:3})
CREATE (r)-[:ROTATE {val:'R'}]->(d)-[:ROTATE {val:'R'}]->
  (l)-[:ROTATE {val:'R'}]->(u)-[:ROTATE {val:'R'}]->(r)
CREATE (r)-[:ROTATE {val:'L'}]->(u)-[:ROTATE {val:'L'}]->
  (l)-[:ROTATE {val:'L'}]->(d)-[:ROTATE {val:'L'}]->(r);

CREATE
  (gn:CubeDirection {type:"GREEN", natural:true}),
  (gr:CubeDirection {type:"GREEN", natural:false}),
  (un:CubeDirection {type:"BLUE", natural:true}),
  (ur:CubeDirection {type:"BLUE", natural:false}),
  (an:CubeDirection:init:Current {type:"BLACK", natural:true}),
  (ar:CubeDirection {type:"BLACK", natural:false});

MATCH (cd:CubeDirection)
UNWIND [{X:2,Y:1},{X:3,Y:1},{X:2,Y:2},{X:1,Y:3},{X:2,Y:3},{X:1,Y:4}] AS face
WITH {X:face.X, Y:face.Y, dir:cd.type, natural:cd.natural} AS box, cd, face
MATCH (a {X:box.X, Y:box.Y})-[dir WHERE type(dir)=box.dir]->(b {X:box.X, Y:box.Y})
OPTIONAL MATCH(a)-[r1 WHERE type(r1) <> type(dir)]->(b)
OPTIONAL MATCH (a)<-[r2 WHERE type(r2) <> type(dir)]-(b)
WITH [{dir:type(r1),ori:box.natural},{dir:type(r2),ori:NOT box.natural}] AS rl_dirs,
  box, cd, face
UNWIND rl_dirs AS rl_dir
WITH rl_dir, box, cd, face
WHERE rl_dir.dir IS NOT NULL
RETURN box, collect(rl_dir)[0] AS rl_dir, cd, face;

MATCH (cd:CubeDirection)
UNWIND [{X:2,Y:1},{X:3,Y:1},{X:2,Y:2},{X:1,Y:3},{X:2,Y:3},{X:1,Y:4}] AS face
WITH {X:face.X, Y:face.Y, dir:cd.type, natural:cd.natural} AS box, cd, face
MATCH (a {X:box.X, Y:box.Y})-[dir WHERE type(dir)=box.dir]->(b {X:box.X, Y:box.Y})
OPTIONAL MATCH(a)-[r1 WHERE type(r1) <> type(dir)]->(b)
OPTIONAL MATCH (a)<-[r2 WHERE type(r2) <> type(dir)]-(b)
WITH [{dir:type(r1),ori:box.natural},{dir:type(r2),ori:NOT box.natural}] AS rl_dirs,
  box, cd, face
UNWIND rl_dirs AS rl_dir
WITH rl_dir, box, cd, face
WHERE rl_dir.dir IS NOT NULL
WITH box, collect(rl_dir)[0] AS rl_dir, cd, face
MATCH (rl_dir_eq:Direction {type: rl_dir.dir, natural: rl_dir.ori})
CREATE (cd)-[:SAME_AS {X:face.X, Y: face.Y}]->(rl_dir_eq);


CALL apoc.periodic.iterate(
'MATCH (t:Tile WHERE t.tile = "#")
RETURN t',
'DETACH DELETE t',{batchSize:100});


//// END OF SETUP

/// RUN
CALL apoc.periodic.commit('
CALL apoc.cypher.runMany(\'
  MATCH (pos:Tile&Current), (step:Step&Current), (dir:CubeDirection&Current)
  WHERE step.type = "rotate"
  MATCH (dir)-[:SAME_AS {X:pos.X, Y:pos.Y}]->(eqdir:Direction)
    -[:ROTATE {val:step.val}]->(new_eq_dir:Direction)
      <-[:SAME_AS {X:pos.X, Y:pos.Y}]-(new_dir:CubeDirection)
  REMOVE dir:Current
  SET new_dir:Current;

  MATCH (pos:Tile&Current), (step:Step&Current), (dir:CubeDirection&Current)
  WHERE step.type = "walk"
  CALL apoc.path.expandConfig(pos,
      {
        relationshipFilter: CASE WHEN dir.natural THEN dir.type+">" ELSE "<"+dir.type END,
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

MATCH (pos:Tile&Current)
MATCH (dir:CubeDirection&Current)-[:SAME_AS {X:pos.X,Y:pos.Y}]->(eqdir:Direction)
RETURN 1000 * pos.y + 4 * pos.x + eqdir.val AS `part 2`;
