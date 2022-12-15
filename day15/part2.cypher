// uncovered point has to be close to some covered area border
CALL apoc.periodic.iterate('MATCH (s:Sensor)-[r:CLOSEST_BEACON]->()
UNWIND range(-(r.d+1), (r.d+1)) AS delta_x
WITH delta_x, r.d+1 - abs(delta_x) AS delta_y, s, r
WITH s.x AS s_x, s.y AS s_y, delta_x, delta_y
WITH s_x + delta_x AS x, [s_y + delta_y, s_y - delta_y] AS ys
UNWIND ys AS y
WITH x, y
WHERE $min_coord <= x <= $max_coord
AND $min_coord <= y <= $max_coord
RETURN x, y',
'WITH x, y
MATCH (s:Sensor)-[r:CLOSEST_BEACON]->()
WITH *, abs(s.x-x)+abs(s.y-y) <= r.d AS covered
WITH x, y, collect(covered) AS covered
WHERE none(c IN covered WHERE c)
CREATE (:Point:Result {x:x,y:y})',
{parallel:true, batchSize: 1000,
params:{min_coord:$min_coord, max_coord:$max_coord}});
