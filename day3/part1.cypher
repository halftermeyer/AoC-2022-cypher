LOAD CSV FROM 'file:///input.txt' AS line
WITH line[0] AS line
WITH size(line)/2 AS half_size, line
WITH
    split(left(line,half_size),'') AS half_1,
    split(right(line,half_size),'') AS half_2, line
WITH apoc.coll.intersection (half_1, half_2)[0] AS intersection
WITH
    'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' AS letters, intersection
WITH apoc.text.indexOf(letters, intersection) +1 AS priority
RETURN sum(priority) AS `part 1`
