:param env => 'input';

CREATE CONSTRAINT valve_name
IF NOT EXISTS
FOR (v:Valve) REQUIRE (v.name) IS NODE KEY;

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
CREATE (me:Unprocessed:Me {left:30, pressure:0, seen:[init.name]})
CREATE (me)-[:AT]->(init);

///// END OF SETUP

CALL apoc.periodic.commit(
"
MATCH (me:Unprocessed)
REMOVE me:Unprocessed
WITH me, me.pressure/(31-me.left) AS avg_pressure
ORDER BY avg_pressure DESC LIMIT 1000
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
CREATE (new_me)-[:AT]->(other_v)
WITH count(*) AS limit
RETURN limit
");


MATCH (n:Me)
WITH n, size(n.seen) AS seen, n.pressure AS pressure
ORDER BY pressure DESC LIMIT 1
RETURN pressure AS `part 1`;
