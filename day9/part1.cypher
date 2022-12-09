:param env => 'test';

MATCH (n) DETACH DELETE n;

CREATE INDEX position_x_y
IF NOT EXISTS
FOR (p:Position) ON (p.x, p.y);

CREATE (:Position:Head:CurrentHead:InitialHead:Tail:CurrentTail:InitialTail
  {x: 0, y: 0});

// Parsing
LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH split(line[0], " ") AS line
WITH line[0] AS direction, toInteger(line[1]) AS stepNb
WITH reduce(s="", x IN range(0, stepNb-1) | s + direction) AS directions
WITH apoc.text.join(collect(directions), "") AS step_dirs
WITH split(step_dirs, "") AS step_dirs
UNWIND step_dirs AS step_dir
WITH step_dir, {L: {dx:-1, dy:0}, R: {dx:1, dy:0}, U: {dx: 0, dy: 1}, D: {dx: 0, dy: -1}}[step_dir] AS delta
CALL {
    WITH step_dir, delta
    MATCH (h:CurrentHead)
    MATCH (t:CurrentTail)

    WITH
      h,
      t,
      step_dir,
      {x: h.x + delta.dx, y: h.y + delta.dy} AS new_h_pos
    MERGE (new_h:Position {
                        x: new_h_pos.x,
                        y: new_h_pos.y
                        })
    MERGE (h)-[:NEXT_HEAD {dir: step_dir}]->(new_h)
    REMOVE h:CurrentHead
    SET new_h:Head:CurrentHead

    WITH
      new_h,
      h,
      t,
      new_h_pos,
      {dx: new_h_pos.x - t.x, dy: new_h_pos.y - t.y} AS vect_th
    WHERE apoc.coll.max([abs(vect_th.dx), abs(vect_th.dy)]) > 1
    WITH new_h, h, t, vect_th,
      {
        dx: sign(vect_th.dx),
        dy: sign(vect_th.dy)
      } AS vect_dt
    MERGE (new_t:Position {
                          x: t.x + vect_dt.dx,
                          y: t.y + vect_dt.dy
                          })
    MERGE (t)-[:NEXT_TAIL]->(new_t)
    MERGE (new_t)-[:FOLLOWS]->(new_h)
    REMOVE t:CurrentTail
    SET new_t:Tail:CurrentTail
}
RETURN *;

MATCH (t:Tail)
RETURN count(t) AS `part 1`;
