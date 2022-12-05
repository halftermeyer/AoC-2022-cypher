MATCH (n) DETACH DELETE n;

// CREATE STACKS BOTTOM
LOAD CSV FROM "file:///"+$env+".txt" AS input FIELDTERMINATOR '\n'
WITH [x IN input | coalesce(x, "---")] AS input
CALL apoc.coll.split(input, "---") YIELD value
WITH collect(value) AS input
WITH input[0] AS stacks
WITH REVERSE(stacks) AS stacks
WITH [x IN range(1,(size(stacks[0])+1)/4) | 1 + 4*(x-1)] AS indices, stacks
FOREACH (ix IN indices | MERGE (:Stack:Top {id: split(stacks[0],"")[ix], ix: ix}));

MATCH (stack:Stack)
LOAD CSV FROM "file:///"+$env+".txt" AS input FIELDTERMINATOR '\n'
WITH [x IN input | coalesce(x, "---")] AS input, stack
CALL apoc.coll.split(input, "---") YIELD value
WITH collect(value) AS input, stack
WITH input[0] AS stacks, stack
WITH [x IN REVERSE(stacks)[1..] | split(x,"")[stack.ix]] AS crates, stack
WITH [x IN crates WHERE x <> " "] AS crates, stack
UNWIND crates AS crate
CALL {
  WITH stack, crate
  MATCH (t:Top WHERE t.id = stack.id)
  CREATE (c:Crate:Top {mark: crate, id: stack.id})
  CREATE (t)-[:IS_BELOW]->(c)
  REMOVE t:Top
};
