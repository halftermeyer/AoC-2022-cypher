MATCH (a)-[:BEATS]->(b)
MERGE (a)-[:ANSWER {code: 'X'}]->(b)
MERGE (b)-[:ANSWER {code: 'Z'}]->(a);
MATCH (a:Gesture)
MERGE (a)-[:ANSWER {code: 'Y'}]->(a);

LOAD CSV FROM 'file:///input.txt' AS line
WITH split(line[0], " ") AS line
WITH line [0] AS opp_move, line [1] AS strategy
MATCH (opp_g:Gesture WHERE opp_g.opp_key = opp_move)-[a:ANSWER WHERE a.code = strategy]->(my_g:Gesture)
RETURN sum (a.val + my_g.val) AS `part 2`;
