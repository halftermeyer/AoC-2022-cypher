:param env => 'input';

CALL apoc.periodic.iterate("MATCH (n) RETURN n", "DETACH DELETE n",{batchSize:10000});

CREATE RANGE INDEX state_costs
IF NOT EXISTS
FOR (s:State) ON (s.blueprint, s.ore, s.clay, s.obsidian, s.minute);

CREATE RANGE INDEX action_costs
IF NOT EXISTS
FOR (a:Action) ON (a.blueprint, a.ore, a.clay, a.obsidian);

LOAD CSV FROM 'file:///'+$env+'.txt' AS lines FIELDTERMINATOR '\n'
UNWIND lines AS line
WITH line
LIMIT 3
WHERE line IS NOT null
WITH apoc.text.regexGroups(
  line,
  'Blueprint (\\d+): Each ore robot costs (\\d+) ore. Each clay robot costs (\\d+) ore. Each obsidian robot costs (\\d+) ore and (\\d+) clay. Each geode robot costs (\\d+) ore and (\\d+) obsidian.'
)[0][1..] AS line
WITH
  toInteger(line[0]) AS blueprintId, line
WITH [
  {ore:-toInteger(toInteger(line[1])), oreBot:1, blueprint: blueprintId, action: "spawnOreRobot"},
  {ore:-toInteger(toInteger(line[2])), clayBot:1, blueprint: blueprintId, action: "spawnClayRobot"},
  {ore:-toInteger(toInteger(line[3])), clay:-toInteger(line[4]), obsidianBot:1, blueprint: blueprintId, action: "spawnObsidianRobot"},
  {ore:-toInteger(toInteger(line[5])), obsidian:-toInteger(line[6]), geodeBot:1, blueprint: blueprintId, action: "spawnGeodeRobot"},
  {blueprint: blueprintId, action: "doNothing"}
] AS actions, blueprintId
CREATE (b:Blueprint {id:blueprintId})
WITH b, actions
UNWIND actions AS action
CREATE (a:Action)
SET a = action
CREATE (b)-[:HAS_ACTION]->(a);

MATCH (b:Blueprint)
CREATE (:State:Unprocessed {
    blueprint: b.id,
    minute:0,
    ore: 0,
    clay: 0,
    obsidian: 0,
    geodes: 0,
    oreBot: 1,
    clayBot:0,
    obsidianBot:0,
    geodeBot:0
});

MATCH (b:Blueprint)-->(a:Action)
WITH b, a.ore AS ore
ORDER BY ore ASC
WHERE ore IS NOT null
WITH b, collect(ore)[0] AS ore
SET b.maxOreBot = -ore;

MATCH (b:Blueprint)-->(a:Action)
WITH b, a.obsidian AS obsidian
ORDER BY obsidian ASC
WHERE obsidian IS NOT null
WITH b, collect(obsidian)[0] AS obsidian
SET b.maxObsidianBot = -obsidian;

MATCH (b:Blueprint)-->(a:Action)
WITH b, a.clay AS clay
ORDER BY clay ASC
WHERE clay IS NOT null
WITH b, collect(clay)[0] AS clay
SET b.maxClayBot = -clay;

//// LOOP 32 TIMES ////

// pruning
MATCH (s:Unprocessed)
MATCH (b:Blueprint {id:s.blueprint})
WITH s, b, s.blueprint AS blueprint,
s.geodeBot AS g,
s.obsidianBot AS ob,
s.clayBot AS c,
s.oreBot AS o,
b.maxOreBot AS maxo,
b.maxClayBot AS maxc,
b.maxObsidianBot AS maxob
WHERE ob > maxob OR c > maxc OR o > maxo
REMOVE s:Unprocessed
SET s:Pruned:ToManyBotsOfAKind;

CALL apoc.periodic.iterate('MATCH (s:Unprocessed)<-[:NEXT {action:"spawnGeodeRobot"}]
  -()-[r:NEXT WHERE r.action <> "spawnGeodeRobot"]->(other:Unprocessed)
RETURN other',
'SET other:Pruned:GeodeRobotBefore
REMOVE other:Unprocessed', {batchSize:1000});

// CALL apoc.periodic.iterate(
// "MATCH (s:Unprocessed)
// MATCH (b:Blueprint {id:s.blueprint})
// WITH s, b,
// b.maxObsidianBot - s.obsidianBot AS ob,
// b.maxClayBot - s.clayBot AS c,
// b.maxOreBot - s.oreBot AS o
// WITH b, {heur:ob + c + o, state: s} AS heur_s, ob + c + o AS heur
// ORDER BY heur ASC
// WITH b, collect(heur_s) AS heur_s, collect(heur_s)[0].heur AS min_heur
// UNWIND heur_s AS hs
// WITH b, min_heur, hs.heur AS heur, hs.state AS s
// WHERE heur > min_heur + 3
// RETURN s",
// "REMOVE s:Unprocessed
// SET s:Pruned:ToFewRobots;",{batchSize:1000});


// heuristics
MATCH (s:Unprocessed)
MATCH (b:Blueprint {id:s.blueprint})
WITH s, b,
10_000_000 * (s.geodes + s.geodeBot*(32-s.minute)) AS g,
10_000 * s.obsidianBot AS ob,
100 * s.clayBot AS c,
s.oreBot AS o
WITH s, b, g+ob+c+o AS fitness
ORDER BY fitness DESC
WITH collect(s) AS states, b
UNWIND states[5000..] AS s
REMOVE s:Unprocessed
SET s:Pruned:Heuristics;


// CALL apoc.periodic.iterate("MATCH (s:State&!Unprocessed)
// RETURN s","
// DETACH DELETE s",{batchSize:1000});

CALL apoc.periodic.iterate(
  'MATCH (s:State&Unprocessed WHERE s.minute <= 32)
  RETURN s',
  'MATCH (a:Action)
  WHERE a.blueprint = s.blueprint
  AND coalesce(a.ore, 0) + s.ore >= 0
  AND coalesce(a.clay, 0) + s.clay >= 0
  AND coalesce(a.obsidian, 0) + s.obsidian >= 0
  MERGE (new_s:State:Unprocessed {
    minute: s.minute + 1,
    ore: s.ore + s.oreBot + coalesce(a.ore, 0),
    clay: s.clay + s.clayBot + coalesce(a.clay, 0),
    obsidian: s.obsidian + s.obsidianBot + coalesce(a.obsidian, 0),
    geodes: s.geodes + s.geodeBot,
    oreBot: s.oreBot + coalesce(a.oreBot, 0),
    clayBot: s.clayBot + coalesce(a.clayBot, 0),
    obsidianBot: s.obsidianBot + coalesce(a.obsidianBot, 0),
    geodeBot: s.geodeBot + coalesce(a.geodeBot, 0),
    blueprint: s.blueprint
  })
  CREATE (s)-[:NEXT {action:a.action, blueprint:a.blueprint}]->(new_s)
  REMOVE s:Unprocessed;',{parallel:false, batchSize:500});



///// END LOOP //////

MATCH (n:Unprocessed)
WITH n.blueprint AS blueprint, n.geodes AS geodes
ORDER BY geodes DESC
WITH blueprint, collect(geodes)[0] AS geodes
WITH collect(geodes) AS geodes
RETURN reduce (acc=1, g IN geodes | acc * g) AS `part 2`;

//15288
//24192
