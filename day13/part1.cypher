:param env => 'input';

MATCH (n) DETACH DELETE n;

CREATE INDEX packet_val
IF NOT EXISTS
FOR (p:Packet) ON (p.val);

LOAD CSV FROM 'file:///'+$env+'.txt' AS lines FIELDTERMINATOR "\n"
WITH [ix IN range(0, size(lines)-1)|{ix:ix, val:lines[ix], pair:1 + ix/3}] AS lines
UNWIND lines AS packet
WITH packet
WHERE packet.val IS NOT null
CREATE (p:Packet)
SET p = packet;

MATCH (p:Packet)
WITH DISTINCT (p.pair) AS pair
CREATE (p:Pair:Root {pair: pair});

MATCH (pair:Pair)
WITH pair, pair.pair AS pairid
MATCH(pack:Packet WHERE pack.pair = pairid)
WITH pair, pack, pack.ix AS ix
ORDER BY ix
WITH pair, collect(pack) AS packs
WITH pair, packs[0] AS left, packs[1] AS right
CREATE (pair)-[:LEFT]->(left)
CREATE (pair)-[:RIGHT]->(right);

MATCH (p:Packet)
WITH p.val AS val, p
WITH p.val AS val, collect(p) AS packs
CALL apoc.refactor.mergeNodes(packs, {properties:"combine", mergeRels:true}) YIELD node
RETURN count(*);
////////

CALL apoc.periodic.commit('
CALL apoc.cypher.runMany(\'
// rule 1_distinct
MATCH (left:Packet)<-[:LEFT]-(pair:Pair&!TieBreaker&!Decided)-[:RIGHT]->(right:Packet)
WHERE NOT EXISTS {(pair)-[:SUB_PAIR]->(:!TieBreaker)}
CALL apoc.cypher.run("RETURN "+left.val+" AS obj_val", {})
YIELD value
WITH pair, left, right, value.obj_val AS left_obj_val
CALL apoc.cypher.run("RETURN "+right.val+" AS obj_val", {})
YIELD value
WITH
    pair, left, right, left_obj_val,
    value.obj_val AS right_obj_val
WITH *,
    apoc.meta.cypher.type(left_obj_val) AS left_type,
    apoc.meta.cypher.type(right_obj_val) AS right_type,
    apoc.convert.toJson(left_obj_val) AS left_obj_str_val,
    apoc.convert.toJson(right_obj_val) AS right_obj_str_val
WHERE left_type = "INTEGER"
AND right_type = "INTEGER"
AND left_obj_val <> right_obj_val
CREATE (sub:Pair:Decided {right_order: left_obj_val < right_obj_val})
CREATE (pair)-[:SUB_PAIR {rule:"1_distint"}]->(sub);

// rule 1_equal
MATCH (left:Packet)<-[:LEFT]-(pair:Pair&!TieBreaker&!Decided)-[:RIGHT]->(right:Packet)
WHERE NOT EXISTS {(pair)-[:SUB_PAIR]->(:!TieBreaker)}
CALL apoc.cypher.run("RETURN "+left.val+" AS obj_val", {})
YIELD value
WITH pair, left, right, value.obj_val AS left_obj_val
CALL apoc.cypher.run("RETURN "+right.val+" AS obj_val", {})
YIELD value
WITH
    pair, left, right, left_obj_val,
    value.obj_val AS right_obj_val
WITH *,
    apoc.meta.cypher.type(left_obj_val) AS left_type,
    apoc.meta.cypher.type(right_obj_val) AS right_type,
    apoc.convert.toJson(left_obj_val) AS left_obj_str_val,
    apoc.convert.toJson(right_obj_val) AS right_obj_str_val
WHERE left_type = "INTEGER"
AND right_type = "INTEGER"
AND left_obj_val = right_obj_val
CREATE (sub:Pair:Tie)
CREATE (pair)-[:SUB_PAIR {rule:"1_equal"}]->(sub);

//rule 2 right_out_of_items
MATCH (left:Packet)<-[:LEFT]-(pair:Pair&!TieBreaker&!Decided)-[:RIGHT]->(right:Packet)
WHERE NOT EXISTS {(pair)-[:SUB_PAIR]->(:!TieBreaker)}
CALL apoc.cypher.run("RETURN "+left.val+" AS obj_val", {})
YIELD value
WITH pair, left, right, value.obj_val AS left_obj_val
CALL apoc.cypher.run("RETURN "+right.val+" AS obj_val", {})
YIELD value
WITH
    pair, left, right, left_obj_val,
    value.obj_val AS right_obj_val
WITH *,
    apoc.meta.cypher.type(left_obj_val) AS left_type,
    apoc.meta.cypher.type(right_obj_val) AS right_type,
    apoc.convert.toJson(left_obj_val) AS left_obj_str_val,
    apoc.convert.toJson(right_obj_val) AS right_obj_str_val
WHERE right_type STARTS WITH "LIST"
AND  left_type STARTS WITH "LIST"
AND size(left_obj_val) > 0
AND size(right_obj_val) = 0
CREATE (sub:Pair:Decided {right_order: false})
CREATE (pair)-[:SUB_PAIR {rule:"2_right_out_of_items"}]->(sub);

//rule 2 left_out_of_items
MATCH (left:Packet)<-[:LEFT]-(pair:Pair&!TieBreaker&!Decided)-[:RIGHT]->(right:Packet)
WHERE NOT EXISTS {(pair)-[:SUB_PAIR]->(:!TieBreaker)}
CALL apoc.cypher.run("RETURN "+left.val+" AS obj_val", {})
YIELD value
WITH pair, left, right, value.obj_val AS left_obj_val
CALL apoc.cypher.run("RETURN "+right.val+" AS obj_val", {})
YIELD value
WITH
    pair, left, right, left_obj_val,
    value.obj_val AS right_obj_val
WITH *,
    apoc.meta.cypher.type(left_obj_val) AS left_type,
    apoc.meta.cypher.type(right_obj_val) AS right_type,
    apoc.convert.toJson(left_obj_val) AS left_obj_str_val,
    apoc.convert.toJson(right_obj_val) AS right_obj_str_val
WHERE right_type STARTS WITH "LIST"
AND  left_type STARTS WITH "LIST"
AND size(right_obj_val) > 0
AND size(left_obj_val) = 0
CREATE (sub:Pair:Decided {right_order: true})
CREATE (pair)-[:SUB_PAIR {rule:"2_left_out_of_items"}]->(sub);

//rule 2 both_out_of_items
MATCH (left:Packet)<-[:LEFT]-(pair:Pair&!TieBreaker&!Decided)-[:RIGHT]->(right:Packet)
WHERE NOT EXISTS {(pair)-[:SUB_PAIR]->(:!TieBreaker)}
CALL apoc.cypher.run("RETURN "+left.val+" AS obj_val", {})
YIELD value
WITH pair, left, right, value.obj_val AS left_obj_val
CALL apoc.cypher.run("RETURN "+right.val+" AS obj_val", {})
YIELD value
WITH
    pair, left, right, left_obj_val,
    value.obj_val AS right_obj_val
WITH *,
    apoc.meta.cypher.type(left_obj_val) AS left_type,
    apoc.meta.cypher.type(right_obj_val) AS right_type,
    apoc.convert.toJson(left_obj_val) AS left_obj_str_val,
    apoc.convert.toJson(right_obj_val) AS right_obj_str_val
WHERE right_type STARTS WITH "LIST"
AND  left_type STARTS WITH "LIST"
AND size(right_obj_val) = 0
AND size(left_obj_val) = 0
CREATE (sub:Pair:Tie)
CREATE (pair)-[:SUB_PAIR {rule:"2_both_out_of_items"}]->(sub);

//rule 2 both provide
MATCH (left:Packet)<-[:LEFT]-(pair:Pair&!TieBreaker&!Decided)-[:RIGHT]->(right:Packet)
WHERE NOT EXISTS {(pair)-[:SUB_PAIR]->(:!TieBreaker)}
CALL apoc.cypher.run("RETURN "+left.val+" AS obj_val", {})
YIELD value
WITH pair, left, right, value.obj_val AS left_obj_val
CALL apoc.cypher.run("RETURN "+right.val+" AS obj_val", {})
YIELD value
WITH
    pair, left, right, left_obj_val,
    value.obj_val AS right_obj_val
WITH *,
    apoc.meta.cypher.type(left_obj_val) AS left_type,
    apoc.meta.cypher.type(right_obj_val) AS right_type,
    apoc.convert.toJson(left_obj_val) AS left_obj_str_val,
    apoc.convert.toJson(right_obj_val) AS right_obj_str_val
WHERE right_type STARTS WITH "LIST"
AND  left_type STARTS WITH "LIST"
AND size(left_obj_val) > 0
AND size(right_obj_val) > 0
WITH *,
    left_obj_val[0] AS left_head,
    right_obj_val[0] AS right_head,
    left_obj_val[1..] AS left_tail,
    right_obj_val[1..] AS right_tail
WITH *,
    apoc.convert.toJson(left_head) AS left_head_obj_str,
    apoc.convert.toJson(right_head) AS right_head_obj_str,
    apoc.convert.toJson(left_tail) AS left_tail_obj_str,
    apoc.convert.toJson(right_tail) AS right_tail_obj_str
MERGE (new_left_head:Packet {val: left_head_obj_str})
MERGE (new_right_head:Packet {val: right_head_obj_str})
MERGE (new_left_tail:Packet {val: left_tail_obj_str})
MERGE (new_right_tail:Packet {val: right_tail_obj_str})
CREATE (sub:Pair)
CREATE (pair)-[:SUB_PAIR {rule:"2_both_provide"}]->(sub)
CREATE (tieBreaker:Pair:TieBreaker)
CREATE (sub)-[:SUB_PAIR {rule:"2_both_provide_tie_breaker"}]->(tieBreaker)
CREATE (sub)-[:RIGHT]->(new_right_head)
CREATE (sub)-[:LEFT]->(new_left_head)
CREATE (tieBreaker)-[:RIGHT]->(new_right_tail)
CREATE (tieBreaker)-[:LEFT]->(new_left_tail);


// rule 3_left
MATCH (left:Packet)<-[:LEFT]-(pair:Pair&!TieBreaker&!Decided)-[:RIGHT]->(right:Packet)
WHERE NOT EXISTS {(pair)-[:SUB_PAIR]->(:!TieBreaker)}
CALL apoc.cypher.run("RETURN "+left.val+" AS obj_val", {})
YIELD value
WITH pair, left, right, value.obj_val AS left_obj_val
CALL apoc.cypher.run("RETURN "+right.val+" AS obj_val", {})
YIELD value
WITH
    pair, left, right, left_obj_val,
    value.obj_val AS right_obj_val
WITH *,
    apoc.meta.cypher.type(left_obj_val) AS left_type,
    apoc.meta.cypher.type(right_obj_val) AS right_type,
    apoc.convert.toJson(left_obj_val) AS left_obj_str_val,
    apoc.convert.toJson(right_obj_val) AS right_obj_str_val
WHERE left_type = "INTEGER"
AND NOT right_type = "INTEGER"
CREATE (sub:Pair)
CREATE (pair)-[:SUB_PAIR {rule:"3_left"}]->(sub)
MERGE (new_left:Packet {val: "["+left_obj_str_val+"]"})
CREATE (sub)-[:RIGHT]->(right)
CREATE (sub)-[:LEFT]->(new_left)
CREATE (left)-[:TRANSFORMED {rule:"3_left"}]->(new_left);

// rule 3_right
MATCH (left:Packet)<-[:LEFT]-(pair:Pair&!TieBreaker&!Decided)-[:RIGHT]->(right:Packet)
WHERE NOT EXISTS {(pair)-[:SUB_PAIR]->(:!TieBreaker)}
CALL apoc.cypher.run("RETURN "+left.val+" AS obj_val", {})
YIELD value
WITH pair, left, right, value.obj_val AS left_obj_val
CALL apoc.cypher.run("RETURN "+right.val+" AS obj_val", {})
YIELD value
WITH
    pair, left, right, left_obj_val,
    value.obj_val AS right_obj_val
WITH *,
    apoc.meta.cypher.type(left_obj_val) AS left_type,
    apoc.meta.cypher.type(right_obj_val) AS right_type,
    apoc.convert.toJson(left_obj_val) AS left_obj_str_val,
    apoc.convert.toJson(right_obj_val) AS right_obj_str_val
WHERE right_type = "INTEGER"
AND NOT left_type = "INTEGER"
CREATE (sub:Pair)
CREATE (pair)-[:SUB_PAIR {rule:"3_right"}]->(sub)
MERGE (new_right:Packet {val: "["+right_obj_str_val+"]"})
CREATE (sub)-[:RIGHT]->(new_right)
CREATE (sub)-[:LEFT]->(left)
CREATE (right)-[:TRANSFORMED {rule:"3_right"}]->(new_right);

// recurrence
MATCH (p:Pair&!Decided)-[:SUB_PAIR]->
  (sub:Pair&Decided)
SET
  p.right_order = sub.right_order,
  p:Decided;

MATCH (p:Pair&!Decided&!Tie)-[:SUB_PAIR]->
  (sub:Pair&Tie)
WHERE NOT EXISTS {(p)-[:SUB_PAIR]->(:TieBreaker|Decided)}
SET p:Tie;

MATCH (p:Pair&!Decided&!Tie)-[:SUB_PAIR]->
  (sub:Pair&Tie),
  (p)-[:SUB_PAIR]->(tb:TieBreaker)
REMOVE tb:TieBreaker;
\', {}) YIELD result
WITH count(result) AS res
MATCH (r:Root&!Decided)
WITH count(r) AS limit
RETURN limit',{});

MATCH (s:Root)
WITH s.pair as pair
WHERE s.right_order
RETURN sum(pair) AS `part 1`;
