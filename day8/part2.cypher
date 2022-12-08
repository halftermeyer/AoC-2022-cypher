MATCH (n:Tree)
MATCH p=(n)-[:SOUTH_NEIGHBOR*0..]->()
WITH n, nodes(p) AS no
WHERE all(ix IN range(1, size(no)-2)  WHERE no[ix].height < n.height)
WITH n, no, size(no)-1 AS viewing_distance
ORDER BY viewing_distance DESC
WITH n, collect(viewing_distance) AS viewing_distances
SET n.south_horizon_height = viewing_distances[0];

MATCH (n:Tree)
MATCH p=(n)<-[:SOUTH_NEIGHBOR*0..]-()
WITH n, nodes(p) AS no
WHERE all(ix IN range(1, size(no)-2)  WHERE no[ix].height < n.height)
WITH n, no, size(no)-1 AS viewing_distance
ORDER BY viewing_distance DESC
WITH n, collect(viewing_distance) AS viewing_distances
SET n.north_horizon_height = viewing_distances[0];

MATCH (n:Tree)
MATCH p=(n)-[:EAST_NEIGHBOR*0..]->()
WITH n, nodes(p) AS no
WHERE all(ix IN range(1, size(no)-2)  WHERE no[ix].height < n.height)
WITH n, no, size(no)-1 AS viewing_distance
ORDER BY viewing_distance DESC
WITH n, collect(viewing_distance) AS viewing_distances
SET n.east_horizon_height = viewing_distances[0];

MATCH (n:Tree)
MATCH p=(n)<-[:EAST_NEIGHBOR*0..]-()
WITH n, nodes(p) AS no
WHERE all(ix IN range(1, size(no)-2)  WHERE no[ix].height < n.height)
WITH n, no, size(no)-1 AS viewing_distance
ORDER BY viewing_distance DESC
WITH n, collect(viewing_distance) AS viewing_distances
SET n.west_horizon_height = viewing_distances[0];

MATCH (n:Tree)
SET n.scenic_score =
    n.north_horizon_height *
    n.south_horizon_height *
    n.east_horizon_height *
    n.west_horizon_height;

MATCH (n:Tree)
RETURN max(n.scenic_score) AS `part 2`
