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


//// LOOP 32 TIMES ////

// heuristics
MATCH (s:Unprocessed WHERE s.minute <= 32)
WITH s, s.blueprint AS blueprint,
s.geodes + s.geodeBot * (32 - s.minute) AS g,
s.obsidian + s.obsidianBot * (32 - s.minute) AS ob,
s.clay + s.clayBot * (32 - s.minute) AS c,
s.ore + s.oreBot * (32 - s.minute) AS or
ORDER BY g DESC, ob DESC, c DESC, or DESC
WITH collect(s) AS states, blueprint
UNWIND states[2000..] AS s
DETACH DELETE s;

CALL apoc.periodic.iterate("MATCH (s:State&!Unprocessed)
RETURN s","
DETACH DELETE s",{batchSize:1000});

CALL apoc.periodic.iterate(
  'MATCH (s:State&Unprocessed WHERE s.minute <= 32)
  RETURN s',
  'MATCH (a:Action)
  WHERE a.blueprint = s.blueprint
  AND coalesce(a.ore, 0) + s.ore >= 0
  AND coalesce(a.clay, 0) + s.clay >= 0
  AND coalesce(a.obsidian, 0) + s.obsidian >= 0
  CREATE (new_s:State:Unprocessed {
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
  REMOVE s:Unprocessed;',{parallel:false, batchSize:500});


///// END LOOP //////

MATCH (n:Unprocessed)
WITH n.blueprint AS blueprint, n.geodes AS geodes
ORDER BY blueprint, geodes
WITH blueprint, collect(geodes)[0] AS geodes
WITH collect(geodes) AS geodes
RETURN reduce (acc=1, g IN geodes | acc * g) AS `part 2`;
