CREATE (r:Gesture {name:"Rock", opp_key: "A", my_key: "X", val:1})-[:BEATS]->
  (s:Gesture {name:"Scissors", opp_key: "C", my_key: "Z", val:3})-[:BEATS]->
    (p:Gesture {name:"Paper", opp_key: "B", my_key: "Y", val:2})-[:BEATS]->(r);

LOAD CSV FROM 'file:///input.txt' AS line
WITH split(line[0], " ") AS line
WITH line [0] AS opp_move, line [1] AS my_move
OPTIONAL MATCH (opp_g:Gesture WHERE opp_g.opp_key = opp_move), (my_g:Gesture WHERE my_g.my_key = my_move)
WITH
    opp_g,
    my_g,
    EXISTS {(opp_g)-[:BEATS]->(my_g)} AS loss,
    EXISTS {(opp_g)<-[:BEATS]-(my_g)} AS won,
    id(opp_g) = id(my_g) AS draw
WITH
    0 AS loss_score,
    CASE draw WHEN true THEN 3 ELSE 0 END AS draw_score,
    CASE won WHEN true THEN 6 ELSE 0 END AS won_score,
    my_g.val AS shape_score
WITH loss_score + draw_score + won_score + shape_score AS score
RETURN sum(score) AS `part1`;
