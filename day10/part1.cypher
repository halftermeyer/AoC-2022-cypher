//input or test
:param env => 'test';

MATCH (n) DETACH DELETE n;

CREATE INDEX instr_id
IF NOT EXISTS
FOR (i:Instr) ON (i.id);

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH split(line[0], ' ') AS instr, linenumber() AS id
WHERE size(instr) = 1
CREATE (:Instr:Noop {id:id, cycles_taken:1, addX:0});

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH split(line[0], ' ') AS instr, linenumber() AS id
WHERE size(instr) = 2
CREATE (:Instr:AddX
    {id:id, cycles_taken:2, addX:toInteger(instr[1])});

CREATE (:Instr:Noop:Processed {id:0, cycles_taken:1, addX:0, from_cycle: 0, to_cycle:0, X:1});

MATCH (i:Instr)
WITH i, i.id AS id ORDER BY id
WITH collect(i) AS instrs
CALL apoc.nodes.link(instrs, "NEXT");

CALL apoc.periodic.commit("
MATCH (prev:Instr&Processed)-[:NEXT]->(instr:Instr&!Processed)
SET
  instr.from_cycle = prev.to_cycle + 1,
  instr.to_cycle = prev.to_cycle + instr.cycles_taken,
  instr.X = prev.X + prev.addX,
  instr:Processed
WITH count(*) AS limit
RETURN limit
",{});

UNWIND [20, 60, 100, 140, 180, 220] AS cycle
MATCH (i:Instr WHERE i.from_cycle <= cycle <= i.to_cycle)
WITH cycle, i.X AS X, cycle * i.X AS sigStrength
RETURN sum(sigStrength) AS `part 1`;
