LOAD CSV FROM 'file:///input.txt' AS line FIELDTERMINATOR '\n'
WITH [x IN line | toInteger(coalesce(x, "-1"))] AS line
CALL apoc.coll.split(line, -1) YIELD value
WITH value AS inventory
WITH toInteger(apoc.coll.sum(inventory)) AS cals ORDER BY cals DESC
WITH cals LIMIT 3
RETURN sum(cals) AS `result part 2`
