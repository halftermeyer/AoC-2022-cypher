:param env => 'input';
:param rowToCheck => 2000000;

MATCH (n) DETACH DELETE n;

CREATE CONSTRAINT point_x_y
IF NOT EXISTS
FOR (p:Point) REQUIRE (p.x, p.y) IS NODE KEY;

// parse
LOAD CSV FROM 'file:///'+$env+'.txt' AS lines FIELDTERMINATOR '\n'
UNWIND lines AS line
WITH line WHERE line IS NOT null
WITH replace(line, ",", "") AS line
WITH replace(line, ":", "") AS line
WITH split(line, " ") AS line
WITH line[2] AS sensor_x, line[3] AS sensor_y, line[8] AS beacon_x, line[9] AS beacon_y
WITH
    toInteger(split(sensor_x, '=')[1]) AS sensor_x,
    toInteger(split(sensor_y, '=')[1]) AS sensor_y,
    toInteger(split(beacon_x, '=')[1]) AS beacon_x,
    toInteger(split(beacon_y, '=')[1]) AS beacon_y
MERGE (s:Point {x: sensor_x, y:sensor_y})
SET s:Sensor
MERGE (b:Point {x: beacon_x, y:beacon_y})
SET b:Beacon
MERGE (s)-[:CLOSEST_BEACON]->(b);

// compute manhattan distance
MATCH (s:Sensor)-[r:CLOSEST_BEACON]->(b:Beacon)
SET r.d = abs(b.x - s.x) + abs(b.y - s.y);

// filter beacon relevant for row to check
MATCH (s:Sensor)-[r:CLOSEST_BEACON]->() // NO TIE
WITH s, s.y - r.d AS min_x_covered, s.y + r.d AS max_x_covered
WHERE $rowToCheck IN range(min_x_covered, max_x_covered)
SET s:Relevant;

// intervals covered
MATCH (s:Sensor&Relevant)-[r:CLOSEST_BEACON]->()
WITH s, r.d AS d
WITH s, {inf:s.x-(d-abs($rowToCheck-s.y)), sup:s.x+(d-abs($rowToCheck-s.y))} AS i
CREATE (interv:XInterval {inf: i.inf, sup: i.sup});

// reduce intervals
CALL apoc.periodic.commit('
MATCH (left:XInterval), (right:XInterval)
WHERE id(left) <> id(right)
AND left.inf <= right.inf <= left.sup + 1
WITH left, right
LIMIT 1
CREATE (fusion:XInterval
    {inf: apoc.coll.min([left.inf, right.inf]), sup: apoc.coll.max([left.sup, right.sup])})
DETACH DELETE left
DETACH DELETE right
WITH count(*) AS limit
RETURN limit',{});

// intervals cardinality

MATCH (i:XInterval)
SET i.cardinality = i.sup - i.inf + 1;

// result

MATCH (b:Beacon WHERE b.y = $rowToCheck)
MATCH (i:XInterval)
WHERE i.inf <= b.x <= i.sup
WITH DISTINCT b
WITH count(b) AS beacons_covered
MATCH (i:XInterval)
WITH sum(i.cardinality) AS points_covered, beacons_covered
RETURN points_covered - beacons_covered AS `part 1`;
