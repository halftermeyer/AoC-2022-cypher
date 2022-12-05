// input or test env
:param env => 'input';

MATCH (n) DETACH DELETE n;

// *** PARSING STACKS ***

// CREATE STACKS BOTTOM
LOAD CSV FROM "file:///"+$env+".txt" AS input FIELDTERMINATOR '\n'
WITH [x IN input | coalesce(x, "---")] AS input
CALL apoc.coll.split(input, "---") YIELD value
WITH collect(value) AS input
WITH input[0] AS stacks
WITH REVERSE(stacks) AS stacks
WITH [x IN range(1,(size(stacks[0])+1)/4) | 1 + 4*(x-1)] AS indices, stacks
FOREACH (ix IN indices | MERGE (:Stack:Top {id: split(stacks[0],"")[ix], ix: ix}));

// STACK STUFFS
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

// *** PARSING PROCEDURE ***
LOAD CSV FROM "file:///"+$env+".txt" AS input FIELDTERMINATOR '\n'
WITH [x IN input | coalesce(x, "---")] AS input
CALL apoc.coll.split(input, "---") YIELD value
WITH collect(value) AS input
WITH input[1] AS steps
UNWIND steps AS step
WITH split(step, " ") AS step
WITH step[1] AS number, step[3] AS from_stack, step[5] AS to_stack
CREATE (step:Step{from_stack: from_stack, to_stack: to_stack, number: toInteger(number)})
WITH collect(step) AS steps
CALL apoc.nodes.link(steps, "NEXT");

CALL apoc.periodic.commit("
MATCH (step:Step WHERE step.number <> 0)
OPTIONAL MATCH (prev)-[:NEXT]->(step)
WITH step, prev
WHERE prev IS null OR prev.number = 0
WITH step.number AS number, step.from_stack AS from_stack, step.to_stack AS to_stack, step
MATCH (:Stack {id: from_stack})-[:IS_BELOW*0..]->(target:Top)
MATCH (new_top)-[old:IS_BELOW]->(target)
SET new_top:Top
DELETE old
WITH number, from_stack, to_stack, target, step
MATCH (:Stack {id: to_stack})-[:IS_BELOW*0..]->(old_top:Top)
REMOVE old_top:Top
CREATE (old_top)-[:IS_BELOW]->(target)
SET step.number = step.number - 1
WITH count(*) AS limit
RETURN limit"
);

MATCH (s:Stack)-[:IS_BELOW*0..]->(top:Top)
WITH s.id AS id, top.mark AS mark ORDER BY id
RETURN apoc.text.join(collect(mark), "") AS `part 1`;
