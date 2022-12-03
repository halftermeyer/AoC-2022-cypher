LOAD CSV FROM 'file:///input.txt' AS line
WITH line[0] AS line
WITH line, linenumber() AS index
WITH line, (index-1) / 3 AS group, index
WITH group, collect(line) AS bags
WITH group, [x IN bags | split(x, '')] AS bags
WITH reduce(s = split('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ',''),
x IN bags | apoc.coll.intersection(s, x))[0] AS group_badge
WITH
    'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' AS letters, group_badge
WITH apoc.text.indexOf(letters, group_badge) +1 AS priority
RETURN sum(priority) AS `part 2`
