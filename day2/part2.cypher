LOAD CSV FROM 'file:///input.txt' AS line
WITH split(line[0], " ") AS line
WITH line [0] AS opp_move, line [1] AS strategy
MATCH (my_losing_g)<-[:BEATS]-(opp_g:Gesture WHERE opp_g.opp_key = opp_move)<-[:BEATS]-(my_winning_g:Gesture)
WITH opp_g,
    CASE strategy
        WHEN 'X' THEN my_losing_g
        WHEN 'Y' THEN opp_g
        WHEN 'Z' THEN my_winning_g
    END AS my_g
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
