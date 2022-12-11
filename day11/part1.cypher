//input or test
:param env => 'input';
:param round => 20;

MATCH (n) DETACH DELETE n;

LOAD CSV FROM 'file:///'+$env+'.txt' AS line FIELDTERMINATOR "\n"
CALL apoc.coll.split(line, null)
YIELD value
WITH {
    monkey: toInteger(split(split(value[0], " ")[1], ":")[0]),
    items: [item IN split(substring(value[1],18), ", ") | toInteger(item)],
    operator: split(value[2], " ")[-2],
    operand: split(value[2], " ")[-1],
    test_div: toInteger(split(value[3], " ")[-1]),
    monkey_true: toInteger(split(value[4], " ")[-1]),
    monkey_false: toInteger(split(value[5], " ")[-1]),
    inspected_total: 0,
    round: 0
} AS monkey
CREATE (m:Monkey) SET m = monkey;

MATCH (m:Monkey)
MATCH (m_true:Monkey WHERE m_true.monkey = m.monkey_true)
CREATE (m)-[:THROWS_TO {when: true}]->(m_true);
MATCH (m:Monkey)
MATCH (m_false:Monkey WHERE m_false.monkey = m.monkey_false)
CREATE (m)-[:THROWS_TO {when: false}]->(m_false);


CALL apoc.periodic.commit('
  MATCH (m:Monkey WHERE m.round < $round)
  WITH m.round AS round, m.monkey AS monkey, m
  ORDER BY round, monkey LIMIT 1
  SET m.inspected_total = m.inspected_total + size(m.items)
  SET m.round = m.round + 1
  WITH m, m.items AS items
  SET m.items = []
  WITH m, items
  UNWIND items AS item
  CALL apoc.cypher.run(apoc.text.replace(
    "RETURN " +item + " " +  m.operator + " " + m.operand + " AS new", "old",
    "" + item),
    {})
  YIELD value
  WITH value.new / 3 AS new_val, m
  WITH new_val % m.test_div = 0 AS test, new_val, m
  OPTIONAL MATCH (m)-[:THROWS_TO {when: test}]->(other:Monkey)
  WITH m, other, collect(new_val) AS new_items
  SET other.items = other.items + new_items
  WITH collect(m) AS _
  WITH COUNT {(m:Monkey WHERE m.round < $round)} AS limit
  RETURN limit
',{round: $round});

MATCH (m:Monkey)
WITH m, m.inspected_total AS inspected
ORDER BY inspected DESC
LIMIT 2
WITH collect(inspected) AS inspected
RETURN reduce(acc = 1, x IN inspected | acc * x) AS `part 1`
