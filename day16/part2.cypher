:param env => 'input';

CREATE CONSTRAINT valve_name
IF NOT EXISTS
FOR (v:Valve) REQUIRE (v.name) IS NODE KEY;

CREATE INDEX IF NOT EXISTS FOR (m:Me) ON (m.pressure);

CALL apoc.periodic.iterate("MATCH (n) RETURN n","DETACH DELETE n",{batchSize:10000});

// parse
LOAD CSV FROM 'file:///'+$env+'.txt' AS line FIELDTERMINATOR ";"
WITH replace(replace(line[0],"Valve ", ""), " has flow rate=", ",") AS line_1, line
WITH split(line_1, ',') AS line_1, line
WITH line_1, replace(line[1]," tunnels lead to valves ", "") AS line_2
WITH line_1, replace(line_2," tunnel leads to valve ", "") AS line_2
WITH line_1, replace(line_2," ", "") AS line_2
WITH line_1, split(line_2,",") AS line_2
WITH line_1[0] AS valve, toInteger(line_1[1]) AS flowRate, line_2 AS tunnels_to
MERGE (vc:Valve {name:valve})
    SET vc.flowRate = 0
MERGE (vo:Valve:Open {name:valve+"_open"})
    SET vo.flowRate = flowRate
MERGE (vc)-[:OPEN {duration: 1, flowRate: flowRate, valve: valve}]->(vo)
FOREACH (adjv IN tunnels_to |
    MERGE (av:Valve {name:adjv})
    CREATE (vc)-[:TUNNEL {duration: 1}]->(av)
    CREATE (vo)-[:TUNNEL {duration: 1}]->(av));

MATCH (v:Valve {name: "AA"})
SET v:Init;
MATCH (v:Valve:Open WHERE v.flowRate = 0)
DETACH DELETE v;


CALL gds.graph.drop('tunnel_network', false);

CALL gds.graph.project(
  'tunnel_network',
  'Valve',
  {
    TUNNEL: {
      properties: 'duration'
    },
    OPEN: {
      properties: 'duration'
    }
  }
)
YIELD graphName;

CALL gds.alpha.allShortestPaths.stream('tunnel_network', {
  relationshipWeightProperty: 'duration'
})
YIELD sourceNodeId, targetNodeId, distance
WITH sourceNodeId, targetNodeId, distance
WHERE gds.util.isFinite(distance) = true
AND sourceNodeId <> targetNodeId
MATCH (src:Valve WHERE id(src) = sourceNodeId),
(tgt:Valve WHERE id(tgt) = targetNodeId)
MERGE (src)-[:DIRECT {duration:toInteger(distance)}]->(tgt);

MATCH (v:Valve&!(Init|Open)) DETACH DELETE v;


MATCH (init:Init)
CREATE (me:Unprocessed:Me {left:26, pressure:0, seen:[init.name]})
CREATE (me)-[:AT]->(init);

///// END OF SETUP

CALL apoc.periodic.commit(
"
MATCH (me:Unprocessed)
REMOVE me:Unprocessed
//WITH me, me.pressure/(27-me.left) AS avg_pressure
//ORDER BY avg_pressure DESC LIMIT 30000
WITH me
MATCH (me)-[:AT]->(v:Valve)
MATCH (v)-[r:DIRECT]->(other_v:Valve)
WHERE NOT other_v.name IN me.seen
AND me.left-r.duration >=0
CREATE (new_me:Me:Unprocessed
  {
    left: me.left-r.duration,
    pressure: me.pressure + (me.left-r.duration) * other_v.flowRate,
    seen: me.seen + other_v.name
  })
CREATE (me)-[:SPAWNS]->(new_me)
CREATE (new_me)-[:AT]->(other_v)
WITH count(*) AS limit
RETURN limit
");

// CALL apoc.periodic.iterate(
// "MATCH (me:Me)
// RETURN me",
// "UNWIND me.seen AS name
// MATCH (v:Valve WHERE v.name = name)
// CREATE (me)-[:SEEN]->(v)",
// {batchSize:1000, parallel:false});

// CALL apoc.periodic.iterate(
// "MATCH ()-[r:SEEN]->(v:Valve WHERE v.name='AA')
// RETURN r",
// "DELETE r", {batchSize: 1000});

CALL apoc.periodic.iterate("MATCH (me:Me)
UNWIND me.seen AS seen
WITH me, seen
ORDER BY seen
WITH me, collect(seen) AS v_set, me.pressure AS pressure
RETURN v_set, pressure",
"MERGE (c:Combination {valves: v_set})
    ON CREATE SET c.pressure = pressure
    ON MATCH SET c.pressure = apoc.coll.max([c.pressure, pressure])",
{batchSize:1000});

MATCH (c1:Combination WHERE c1.pressure > 1150),(c2:Combination WHERE c2.pressure > 1150)
WHERE none(v IN c1.valves[1..] WHERE v IN c2.valves)
WITH c1.pressure + c2.pressure AS pressure
ORDER BY pressure DESC
RETURN pressure LIMIT 1 AS `part2`;
