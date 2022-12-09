//input, test or test_larger
:param env => 'input';
//number of knots
:param kNb => 10;

MATCH (n) DETACH DELETE n;

CREATE INDEX position_x_y
IF NOT EXISTS
FOR (p:Position) ON (p.x, p.y);

WITH [ix IN range(0,$kNb - 1) | {ix: ix, x: 0, y: 0}] AS knots
UNWIND knots AS k
CREATE (kn:Knot {id:k.ix})
MERGE (pos:Position {x: k.x, y: k.y})
MERGE (kn)-[:IS_AT]->(pos)
SET pos.seen_knots = coalesce(pos.seen_knots, []) + [kn.id];

MATCH (k:Knot)
WITH k, k.id AS id
ORDER BY id DESC
WITH collect(k) AS ks
CALL apoc.nodes.link(ks, "FOLLOWS");

// Parsing
LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH split(line[0], " ") AS line
WITH line[0] AS direction, toInteger(line[1]) AS stepNb
WITH reduce(s="", x IN range(0, stepNb-1) | s + direction) AS directions
WITH apoc.text.join(collect(directions), "") AS step_dirs
WITH split(step_dirs, "") AS step_dirs
UNWIND [ix IN range(0, size(step_dirs)-1) | {ix: ix, delta: step_dirs[ix]}] AS step_dir
WITH step_dir.ix AS ix,
    {
        L: {dx:-1, dy:0},
        R: {dx:1, dy:0},
        U: {dx: 0, dy: 1},
        D: {dx: 0, dy: -1}
    }[step_dir.delta] AS delta
CREATE (s:Step)
SET s.id = ix, s.dx = delta.dx, s.dy = delta.dy
WITH collect(s) AS steps
CALL apoc.nodes.link(steps, "NEXT");


// move knots
MATCH (step:Step)
WITH step.dx AS dx, step.dy AS dy, step.id AS id
ORDER BY id
CALL apoc.cypher.runMany(
    'MATCH (k0:Knot {id:0})-[r:IS_AT]->(pos_0:Position)
    MERGE (new_pos_0:Position {
                        x: pos_0.x + $dx,
                        y: pos_0.y + $dy
                        })
        ON CREATE
            SET new_pos_0.seen_knots = [0]
        ON MATCH
            SET new_pos_0.seen_knots =
                apoc.coll.toSet(new_pos_0.seen_knots + [0])
    DELETE r
    MERGE (k0)-[:IS_AT]->(new_pos_0)
    SET k0:Moved;

    CALL apoc.periodic.commit("
    MATCH (posf:Position)<-[rf:IS_AT]-(kf:Knot&!Moved)-[r:FOLLOWS]->(kl:Knot&Moved)-[rl:IS_AT]->(posl:Position)
    WITH *, {dx: posl.x - posf.x, dy: posl.y - posf.y} AS vect_fl
    SET kf:Moved
    WITH *, apoc.coll.max([abs(vect_fl.dx), abs(vect_fl.dy)]) > 1 AS doesMove
    WITH *,{
            dx: CASE doesMove WHEN true THEN sign(vect_fl.dx) ELSE 0 END,
            dy: CASE doesMove WHEN true THEN sign(vect_fl.dy) ELSE 0 END
          } AS vect_fnewf
    MERGE (new_f_pos:Position {
                              x: posf.x + vect_fnewf.dx,
                              y: posf.y + vect_fnewf.dy
                              })
        ON CREATE
            SET new_f_pos.seen_knots = [kf.id]
        ON MATCH
            SET new_f_pos.seen_knots =
                apoc.coll.toSet(new_f_pos.seen_knots + [kf.id])
    DELETE rf
    MERGE (kf)-[:IS_AT]->(new_f_pos)
    WITH count(kf) AS limit
    RETURN limit", {});
    MATCH (k:Knot)
    REMOVE k:Moved;', {dx:dx, dy:dy}
) YIELD result
//CALL apoc.periodic.commit()
RETURN id, result;

MATCH (p:Position WHERE ($kNb - 1) IN p.seen_knots)
RETURN count(p) AS `part 2`;
