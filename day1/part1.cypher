LOAD CSV FROM 'file:///input.txt' AS line FIELDTERMINATOR '\n'
WITH [x IN line | toInteger(coalesce(x, "-1"))] AS line
CALL apoc.coll.split(line, -1) YIELD value
WITH value AS inventory
RETURN max(toInteger(apoc.coll.sum(inventory))) AS `result part 1`
