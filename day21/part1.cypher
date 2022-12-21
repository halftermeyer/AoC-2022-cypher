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

MATCH (l:Monkey WHERE l.value IS NOT NULL)
  <-[:LEFT]-(m:Monkey WHERE l.value IS NULL)
  -[:RIGHT]->(r:Monkey WHERE l.value IS NOT NULL)
RETURN *

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
')
