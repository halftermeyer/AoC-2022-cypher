:param env => 'input';

CALL apoc.periodic.iterate("MATCH (n) RETURN n",
"DETACH DELETE n", {batchSize:1000});

CREATE CONSTRAINT cube_x_y_z
IF NOT EXISTS
FOR (c:Cube) REQUIRE (c.x,c.y,c.z) IS NODE KEY;

CREATE RANGE INDEX index_name
IF NOT EXISTS
FOR ()-[r:CONSECUTIVE]-() ON (r.dist);

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH toInteger(line[0]) AS x, toInteger(line[1]) AS y, toInteger(line[2]) AS z
CREATE (:Cube {x: x,y: y,z: z});

MATCH (c:Cube)
WITH DISTINCT c.x AS x_max ORDER BY x_max DESC LIMIT 1
MATCH (c:Cube)
WITH DISTINCT x_max, c.y AS y_max ORDER BY y_max DESC LIMIT 1
UNWIND range(0, x_max) AS x
UNWIND range(0, y_max) AS y
WITH x, y
MATCH (c:Cube) WHERE c.x=x AND c.y=y
WITH x, y, c, c.z AS z ORDER BY z
WITH x, y, collect(c) AS cubes
CALL apoc.nodes.link(cubes, "ZCONSECUTIVE");

MATCH (c:Cube)
WITH DISTINCT c.x AS x_max ORDER BY x_max DESC LIMIT 1
MATCH (c:Cube)
WITH DISTINCT x_max, c.z AS z_max ORDER BY z_max DESC LIMIT 1
UNWIND range(0, x_max) AS x
UNWIND range(0, z_max) AS z
WITH x, z
MATCH (c:Cube) WHERE c.x=x AND c.z=z
WITH x, z, c, c.y AS y ORDER BY y
WITH x, z,collect(c) AS cubes
CALL apoc.nodes.link(cubes, "YCONSECUTIVE");

MATCH (c:Cube)
WITH DISTINCT c.y AS y_max ORDER BY y_max DESC LIMIT 1
MATCH (c:Cube)
WITH DISTINCT y_max, c.z AS z_max ORDER BY z_max DESC LIMIT 1
UNWIND range(0, y_max) AS y
UNWIND range(0, z_max) AS z
WITH y, z
MATCH (c:Cube) WHERE c.y=y AND c.z=z
WITH y, z, c, c.x AS x ORDER BY x
WITH y, z, collect(c) AS cubes
CALL apoc.nodes.link(cubes, "XCONSECUTIVE");

MATCH (c1:Cube)-[:XCONSECUTIVE]->(c2:Cube)
MERGE (c1)-[:CONSECUTIVE {axis:"x", dist:c2.x-c1.x}]->(c2);

MATCH (c1:Cube)-[:YCONSECUTIVE]->(c2:Cube)
MERGE (c1)-[:CONSECUTIVE {axis:"y", dist:c2.y-c1.y}]->(c2);

MATCH (c1:Cube)-[:ZCONSECUTIVE]->(c2:Cube)
MERGE (c1)-[:CONSECUTIVE {axis:"z", dist:c2.z-c1.z}]->(c2);

MATCH ()-[r:CONSECUTIVE WHERE r.dist = 1]->()
WITH count(r) AS adjacencies
MATCH (c:Cube)
WITH adjacencies, count(c) AS cubes
RETURN 6*cubes - 2*adjacencies AS `part 1`;
