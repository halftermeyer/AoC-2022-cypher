:param env => 'input';

MATCH (n) DETACH DELETE n;

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH toInteger(line[0]) AS num, linenumber()-1 AS initPos
WITH sign(num) AS sign, abs(num) AS absVal, num, initPos
CREATE (n:Number)
    SET n.num = num, n.initPos = initPos, n.absVal = absVal, n.sign = sign
CREATE (t:PosToken {pos: initPos})
CREATE (n)-[:HAS_TOKEN]->(t);

MATCH (n:Number)
WITH n, n.initPos AS pos ORDER BY pos
WITH collect(n) AS nums
CALL apoc.nodes.link(nums, "NEXT");

MATCH (first:Number WHERE NOT EXISTS {()-[:NEXT]->(first)})
SET first:Current;

MATCH (t:PosToken)
WITH t, t.initPos AS pos ORDER BY pos
WITH collect(t) AS tokens
CALL apoc.nodes.link(tokens, "NEXT");

MATCH (first:PosToken WHERE NOT EXISTS {()-[:NEXT]->(first)})
MATCH (last:PosToken WHERE NOT EXISTS {(last)-[:NEXT]->()})
CREATE (last)-[:NEXT]->(first);


CALL apoc.periodic.commit('
CALL apoc.cypher.runMany(\'
// phase 1
MATCH (:Number) WITH count(*) AS numnum
MATCH (n:Number&Current)-[:HAS_TOKEN]->(t:PosToken)
WITH *,
    CASE
        WHEN n.sign = -1 THEN (numnum -1 - n.absVal) % (numnum - 1)
        ELSE n.absVal % (numnum-1)
    END AS length
CALL apoc.path.expand(t, "NEXT>", null, length, length)
YIELD path
WITH *, nodes(path)[-1] AS target
MATCH (target)-[target_r:NEXT]->(next_target:PosToken)
WITH *, target_r, next_target
DELETE target_r
CREATE (new_t:PosToken:New)
CREATE (target)-[:NEXT]->(new_t)
CREATE (new_t)-[:NEXT]->(next_target)
CREATE (n)-[:HAS_TOKEN]->(new_t);

//phase 2
MATCH (n:Number&Current)-[:HAS_TOKEN]->(old_t:PosToken&!New),
(n:Number&Current)-[:HAS_TOKEN]->(new_t:PosToken&New)
MATCH (prev:PosToken)-[r:NEXT]->(old_t)-[:NEXT]->(next:PosToken)
DETACH DELETE old_t
CREATE (prev)-[:NEXT]->(next)
REMOVE new_t:New;


//phase 3
MATCH (n:Number&Current)-[:NEXT]->(next_n:Number)
REMOVE n:Current
SET next_n:Current
RETURN count(*) AS limit;\',{})
YIELD result

WITH result
WHERE result.limit IS NOT null
WITH result.limit AS limit
RETURN limit',
{});

MATCH (p:PosToken)<-[:HAS_TOKEN]-(n:Number WHERE n.num=0)
UNWIND [1000, 2000, 3000] AS dist
CALL apoc.path.expandConfig(p,
    {relationshipFilter: "NEXT>", minLevel: dist, maxLevel: dist, uniqueness:"NONE"})
YIELD path
WITH nodes(path)[-1] AS target
MATCH (target)<-[:HAS_TOKEN]-(num:Number)
RETURN sum(num.num) AS `part 1`;

// SHOW CURRENT ORDER
// MATCH (p:PosToken)<-[:HAS_TOKEN]-(n:Number WHERE n.num=0)
// WITH p
// MATCH path=(p)-[:NEXT*1..]->(last:PosToken)
// WHERE EXISTS {(last)-[:NEXT]->(p)}
// UNWIND nodes(path) AS token
// MATCH (token)<-[:HAS_TOKEN]-(num:Number)
// RETURN num.num
