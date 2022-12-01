LOAD CSV FROM 'file:///input.txt' AS line FIELDTERMINATOR '\n'
WITH [x IN line | toInteger(coalesce(x, "-1"))] AS line
CALL apoc.coll.split(line, -1) YIELD value
WITH value AS inventory
UNWIND inventory AS cals
WITH inventory, sum(cals) AS cals ORDER BY cals DESC LIMIT 3
RETURN sum(cals) AS `part 2`
