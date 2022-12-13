:param env => 'input';

MATCH (n) DETACH DELETE n;

CALL {
  LOAD CSV FROM 'file:///'+$env+'.txt' AS lines FIELDTERMINATOR "\n"
  WITH [line IN lines|{val:line, divider: false}] AS lines
  UNWIND lines AS packet
  WITH packet
  WHERE packet.val IS NOT null
  RETURN packet
  UNION
  UNWIND [
        {val: "[[2]]", divider: true},
        {val: "[[6]]", divider: true}
      ] AS packet
  RETURN packet
}
WITH packet
CREATE (p:Packet:Init)
SET p = packet;

MATCH (left:Packet:Init),(right:Packet:Init)
WHERE id(left) < id(right)
CREATE (pair:Pair:Root)
CREATE (pair)-[:LEFT]->(left)
CREATE (pair)-[:RIGHT]->(right);

//////


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

////

MATCH (left:Init)<-[:LEFT]-(r:Root)-[:RIGHT]->(right:Init)
WHERE r.right_order
CREATE (left)-[:BEFORE]->(right);
MATCH (left:Init)<-[:LEFT]-(r:Root)-[:RIGHT]->(right:Init)
WHERE NOT r.right_order
CREATE (left)<-[:BEFORE]-(right);

/////

MATCH (div:Init WHERE div.divider)
MATCH (d:Init)
WHERE EXISTS {(d)-[:BEFORE]->(div)}
WITH DISTINCT div, d
WITH div, count(d) + 1 AS div_ix
WITH collect(div_ix) AS div_ixs
RETURN reduce(prod=1, ix IN div_ixs | prod * ix) AS `part2`;
