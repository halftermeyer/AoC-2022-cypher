:param env => 'input';
:param shapes => 'shapes';
:param shape_nb => 2022;

CREATE CONSTRAINT pix_x_y
IF NOT EXISTS
FOR (p:Pix) REQUIRE (p.x, p.y) IS NODE KEY;

CALL apoc.periodic.iterate(
'MATCH (n) RETURN n', 'DETACH DELETE n',
{batchSize:100});

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

MATCH (a)-[:RIGHT]->(b)
CREATE (a)-[:ASIDE {direction: ">"}]->(b)
CREATE (b)-[:ASIDE {direction: "<"}]->(a);

MATCH (p:Pix WHERE p.y = 0)
SET p:Rest:Bottom;
MATCH (p:Pix WHERE p.x IN [0,8])
SET p:Rest:Wall;

CREATE (:Counter {val: 2023, new_sprite: true});

///////// END OF SETUP ///////

CALL apoc.periodic.commit('
  CALL apoc.cypher.runMany(\'

  // GET HEIGTH

  MATCH (c:Counter WHERE c.new_sprite)
  SET c.val = c.val -1, c. new_sprite = false
  WITH c
  MATCH (p:Pix&Rest&!Wall)
  WITH p.y AS y ORDER BY y DESC
  LIMIT 1
  WITH y AS height
  // GET CURRENT SHAPE
  MATCH (rock:RockType&Current)-[:NEXT]->(next_rock)
  WITH *
  REMOVE rock:Current
  SET next_rock:Current
  WITH height, rock
  // CONVERT JSON
  CALL apoc.cypher.run("RETURN "+ replace(rock.sprite, "\\\\"", "") +" AS sprite", {})
  YIELD value
  WITH height, rock, value.sprite AS sprite

  // PLACE SPRITE
  WITH *, 3 AS x_ref, height + 4 AS y_ref
  UNWIND sprite AS pix
  WITH rock, {x: x_ref+ pix.delta_x, y: y_ref+pix.delta_y} AS pix
  MATCH (p:Pix {x: pix.x, y: pix.y})
  SET p:Falling;

  // LET IT MOVE

  MATCH (p:Pix&Falling)
  WITH collect(p) AS ps
  MATCH (jet:Jet&Current)-[:NEXT]->(next_jet:Jet)
  REMOVE jet:Current
  SET next_jet:Current
  WITH ps, jet.direction AS dir
  WHERE none(p IN ps WHERE EXISTS {(p)-[:ASIDE {direction: dir}]->(:Wall|Rest)})
  UNWIND ps AS p
  MATCH (p)-[:ASIDE {direction: dir}]->(new_p:Pix)
  REMOVE p:Falling
  SET new_p:FallingSoon;

  MATCH (p:FallingSoon)
  REMOVE p:FallingSoon
  SET p:Falling;

  MATCH (p:Pix&Falling), (c:Counter)
  WITH c, collect(p) AS ps
  WHERE any(p IN ps WHERE EXISTS {(p)-[:DOWN]->(:Bottom|Rest)})
  SET c.new_sprite = true
  WITH ps
  UNWIND ps AS p
  REMOVE p:Falling
  SET p:Rest;

  MATCH (p:Pix&Falling)
  MATCH (p)-[:DOWN]->(new_p:Pix)
  REMOVE p:Falling
  SET new_p:FallingSoon;

  MATCH (p:FallingSoon)
  REMOVE p:FallingSoon
  SET p:Falling;


  MATCH (c:Counter)
  WITH c.val AS limit
  RETURN limit;\',

  {}) YIELD result
  WITH result
  WHERE result.limit IS NOT null
  WITH result.limit AS limit
  RETURN limit;
');

MATCH (p:Pix&Rest&!Wall)
WITH p.y AS y ORDER BY y DESC
LIMIT 1
RETURN y AS `part 1`;
