:param env => 'test';

MATCH (n) DETACH DELETE n;

CREATE CONSTRAINT monkey_name
IF NOT EXISTS
FOR (m:Monkey) REQUIRE (m.name) IS NODE KEY;

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH split(line[0], ": ") AS line
WITH line[0] AS name, split(line[1], " ") AS says
WHERE size (says) = 1
WITH name, toInteger(says[0]) AS value
MERGE (m:Monkey {name:name})
    SET m.value = value;

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH split(line[0], ": ") AS line
WITH line[0] AS name, split(line[1], " ") AS says
WHERE size (says) = 3
WITH name, says[0] AS left_monkey, says[2] AS right_monkey, says [1] AS op
MERGE (m:Monkey {name:name})
    SET m.op = op
MERGE (l:Monkey {name:left_monkey})
MERGE (r:Monkey {name:right_monkey})
CREATE (m)-[:LEFT]->(l)
CREATE (m)-[:RIGHT]->(r);

MATCH (m:Monkey {name:"root"})
  SET m.op = "=", m.value = true;
MATCH (m:Monkey {name:"humn"})
  REMOVE m.value ;


CALL apoc.periodic.commit('
MATCH (l:Monkey)<-[:LEFT]-(m:Monkey)-[:RIGHT]->(r:Monkey)
WHERE l.value IS NOT NULL
AND m.value IS NULL
AND r.value IS NOT NULL
WITH m, "RETURN " + toString(l.value) + m.op + toString(r.value) + " AS number" AS operation
CALL apoc.cypher.run(operation, {})
YIELD value
SET m.value = value.number
WITH count(*) AS limit
RETURN limit
');

CALL apoc.periodic.commit('
CALL apoc.cypher.runMany(\'
MATCH (l:Monkey)<-[:LEFT]-(m:Monkey {op: "=", value: true})-[:RIGHT]->(r:Monkey)
WHERE l.value IS NOT NULL
AND m.value IS NOT NULL
AND r.value IS NULL
SET r.value = l.value;

MATCH (l:Monkey)<-[:LEFT]-(m:Monkey {op: "=", value: true})-[:RIGHT]->(r:Monkey)
WHERE l.value IS NULL
AND m.value IS NOT NULL
AND r.value IS NOT NULL
SET l.value = r.value;

MATCH (l:Monkey)<-[:LEFT]-(m:Monkey {op: "+"})-[:RIGHT]->(r:Monkey)
WHERE l.value IS NOT NULL
AND m.value IS NOT NULL
AND r.value IS NULL
SET r.value = m.value - l.value;

MATCH (l:Monkey)<-[:LEFT]-(m:Monkey {op: "+"})-[:RIGHT]->(r:Monkey)
WHERE l.value IS NULL
AND m.value IS NOT NULL
AND r.value IS NOT NULL
SET l.value = m.value - r.value;

MATCH (l:Monkey)<-[:LEFT]-(m:Monkey {op: "-"})-[:RIGHT]->(r:Monkey)
WHERE l.value IS NOT NULL
AND m.value IS NOT NULL
AND r.value IS NULL
SET r.value = l.value - m.value;

MATCH (l:Monkey)<-[:LEFT]-(m:Monkey {op: "-"})-[:RIGHT]->(r:Monkey)
WHERE l.value IS NULL
AND m.value IS NOT NULL
AND r.value IS NOT NULL
SET l.value = m.value + r.value;

MATCH (l:Monkey)<-[:LEFT]-(m:Monkey {op: "/"})-[:RIGHT]->(r:Monkey)
WHERE l.value IS NOT NULL
AND m.value IS NOT NULL
AND r.value IS NULL
SET r.value = l.value / m.value;

MATCH (l:Monkey)<-[:LEFT]-(m:Monkey {op: "/"})-[:RIGHT]->(r:Monkey)
WHERE l.value IS NULL
AND m.value IS NOT NULL
AND r.value IS NOT NULL
SET l.value = m.value * r.value;

MATCH (l:Monkey)<-[:LEFT]-(m:Monkey {op: "*"})-[:RIGHT]->(r:Monkey)
WHERE l.value IS NOT NULL
AND m.value IS NOT NULL
AND r.value IS NULL
SET r.value = m.value / l.value;

MATCH (l:Monkey)<-[:LEFT]-(m:Monkey {op: "*"})-[:RIGHT]->(r:Monkey)
WHERE l.value IS NULL
AND m.value IS NOT NULL
AND r.value IS NOT NULL
SET l.value = m.value / r.value;

MATCH (m:Monkey WHERE m.value IS NULL)
WITH count(m) AS limit
RETURN limit
\', {})
YIELD result
WITH result
WHERE result.limit IS NOT null
WITH result.limit AS limit
RETURN limit
');


MATCH (m:Monkey {name:"humn"}) RETURN m.value AS `part 2`;
