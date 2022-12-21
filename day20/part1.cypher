:param env => 'test';

MATCH (n) DETACH DELETE n;

//CREATE (:Log {log:""});

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

MATCH (a:PosToken)-[r:NEXT]->(b:PosToken)
CREATE (b)-[:PREVIOUS]->(a);


CALL apoc.periodic.commit('
CALL apoc.cypher.runMany(\'
// phase 1
MATCH (n:Number&Current)-[:HAS_TOKEN]->(t:PosToken)
MATCH (prev_t)-[r1:NEXT]->(t)-[r2:NEXT]->(next_t)
DETACH DELETE t
CREATE (prev_t)-[:NEXT]->(next_t)
CREATE (next_t)-[:PREVIOUS]->(prev_t)
SET prev_t:StartTokenPos
SET next_t:StartTokenNeg;

// phase 2
MATCH (:Number) WITH count(*) AS numnum
MATCH (n:Number&Current), (start_t_pos:StartTokenPos), (start_t_neg:StartTokenNeg)
WITH n, abs(n.num)%(numnum-1) AS length, n.sign AS sign, start_t_pos, start_t_neg, numnum
WITH *,
CASE
  WHEN sign >= 0
    THEN length
    ELSE length + 1
END AS dist,
  CASE
    WHEN sign >= 0
      THEN "NEXT"
      ELSE "PREVIOUS"
  END AS relType,
  CASE
    WHEN sign >= 0
      THEN start_t_pos
      ELSE start_t_neg
  END AS start_t
CALL apoc.path.expandConfig(start_t,
    {relationshipFilter: relType + ">", minLevel: dist, maxLevel: dist, uniqueness:"NONE"})
YIELD path
WITH *, nodes(path)[-1] AS target
MATCH (target)-[target_r_next:NEXT]->(next_target:PosToken)
MATCH (target)<-[target_r_prev:PREVIOUS]-(next_target)
WITH *, target_r_next, target_r_prev, next_target
DELETE target_r_next
DELETE target_r_prev
CREATE (new_t:PosToken)
CREATE (target)-[:NEXT]->(new_t)
CREATE (target)<-[:PREVIOUS]-(new_t)
CREATE (new_t)-[:NEXT]->(next_target)
CREATE (new_t)<-[:PREVIOUS]-(next_target)
CREATE (n)-[:HAS_TOKEN]->(new_t)
REMOVE start_t_pos:StartTokenPos
REMOVE start_t_neg:StartTokenNeg;

// SHOW CURRENT ORDER ON LOG
//MATCH (p:PosToken)<-[:HAS_TOKEN]-(n:Number WHERE n.num=0)
//WITH p
//MATCH path=(p)-[:NEXT*1..]->(last:PosToken)
//WHERE EXISTS {(last)-[:NEXT]->(p)}
//UNWIND nodes(path) AS token
//MATCH (token)<-[:HAS_TOKEN]-(num:Number)
//WITH apoc.text.join(collect(toString(num.num)), ",")+"\n" AS logLine
//MATCH (log:Log)
//SET log.log = log.log + logLine;

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
