// input or test env
:param env => 'input';

MATCH (n) DETACH DELETE n;

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH line
LIMIT 1
WITH split(line[0], "") AS cs
WITH [ix IN range(0, size(cs)-1)|{ix: ix, sym: cs[ix]}] AS cs
UNWIND cs AS c
CREATE (ch:Char) SET ch = c;

MATCH (c:Char)
WITH c ORDER BY c.ix
WITH collect (c) AS cs
CALL apoc.nodes.link(cs, "NEXT");

MATCH p=(c1)-[*13]->(c4)
WITH size(apoc.coll.toSet([c IN nodes(p) | c.sym])) = 14 AS valid, c4.ix + 1 AS nth
WHERE valid
WITH valid, nth
ORDER BY nth
RETURN nth AS `part 1`
LIMIT 1;
