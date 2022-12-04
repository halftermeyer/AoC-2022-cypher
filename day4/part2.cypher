LOAD CSV FROM "file:///input.txt" AS line FIELDTERMINATOR ","
WITH
  linenumber() AS pair,
  [x IN line | [bound IN split(x,"-")|toInteger(bound)]] AS assignments
WITH
    pair,
    assignments,
    assignments[0][0] AS e1_from,
    assignments[0][1] AS e1_to,
    assignments[1][0] AS e2_from,
    assignments[1][1] AS e2_to
WHERE NOT e1_to < e2_from
    AND NOT e2_to < e1_from
RETURN count(*) AS `part 2`;
