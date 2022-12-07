MATCH (d:Root)
WITH 30000000 - (70000000 - d.size) AS to_free
MATCH (d:Directory WHERE d.size >= to_free)
WITH d.path AS path, d.size AS size
ORDER BY size
LIMIT 1
RETURN size AS `part 2`
