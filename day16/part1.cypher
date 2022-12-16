:param env => 'input';

CREATE CONSTRAINT valve_name
IF NOT EXISTS
FOR (v:Valve) REQUIRE (v.name) IS NODE KEY;

MATCH (n) DETACH DELETE n;

// parse
LOAD CSV FROM 'file:///'+$env+'.txt' AS line FIELDTERMINATOR ";"
WITH replace(replace(line[0],"Valve ", ""), " has flow rate=", ",") AS line_1, line
WITH split(line_1, ',') AS line_1, line
WITH line_1, replace(line[1]," tunnels lead to valves ", "") AS line_2
WITH line_1, replace(line_2," tunnel leads to valve ", "") AS line_2
WITH line_1, replace(line_2," ", "") AS line_2
WITH line_1, split(line_2,",") AS line_2
WITH line_1[0] AS valve, toInteger(line_1[1]) AS flowRate, line_2 AS tunnels_to
MERGE (v:Valve {name:valve})
    SET v.flowRate = flowRate
MERGE (v)-[:OPEN {duration: 1, flowRate: flowRate, valve: valve}]->(v)
FOREACH (adjv IN tunnels_to |
    MERGE (av:Valve {name:adjv})
    CREATE (v)-[:TUNNEL {duration: 1}]->(av));

MATCH (v:Valve {name: "AA"})
SET v:Init;
MATCH (v:Valve WHERE v.flowRate > 0)
SET v:ToOpen;

MATCH (v:Valve&!ToOpen)-[r:OPEN]->(v)
DELETE r;

CALL gds.graph.drop('tunnel_network', false);

CALL gds.graph.project(
  'tunnel_network',
  'Valve',
  {
    TUNNEL: {
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

MATCH (v:Valve&!(Init|ToOpen)) DETACH DELETE v;


MATCH p=(:Valve {name:"AA"})-[DIRECT*15]->(:Valve)
WITH *, relationships(p) AS rels, nodes(p) AS nds
WHERE size(apoc.coll.toSet([n IN nds | id(n)])) = size(nds)
WITH *, [r IN rels | r.duration] AS durations
WITH *, apoc.coll.flatten([d IN durations | [d, 1]]) AS durations
WITH *, [ix IN range(1, size(durations)) |
    toInteger(apoc.coll.sum(durations[0..ix]))] AS from_t
WITH *, [t IN from_t | 30 - t] AS durActive
WITH *, [v In nds | v.flowRate] AS flowRates
WITH *, apoc.coll.flatten([d IN flowRates[1..] | [0, d]]) AS flowRates
WITH *, [ix IN range(0, size(flowRates)-1) | flowRates[ix] * durActive[ix]] AS pressure
RETURN toInteger(apoc.coll.sum(pressure)) AS `part 1`
ORDER BY `part 1` DESC
LIMIT 1
