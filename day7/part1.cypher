// input or test
:param env => 'input';

MATCH (n) DETACH DELETE n;

CREATE CONSTRAINT dir_name_path IF NOT EXISTS
FOR (d:Directory) REQUIRE (d.name, d.path) IS NODE KEY;

CREATE CONSTRAINT file_name_path IF NOT EXISTS
FOR (f:File) REQUIRE (f.name, f.path) IS NODE KEY;

MERGE (:Directory:Current:Root {name: "/", path:"/"});

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH linenumber() AS n, line[0] AS line
WHERE line =~'\$ cd .*' AND line <> "$ cd /"
MATCH (:Directorory)
RETURN n, line;

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH linenumber() AS n, line[0] AS line
WHERE line = "$ cd /"
WITH "MATCH (cur:Directory),(root:Directory:Root)
REMOVE cur:Current
SET root:Current;" AS cypherCode, n, line AS bashCode
CREATE (:TerminalLine {n:n, cypherCode: cypherCode, bashCode: bashCode});

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH linenumber() AS n, line[0] AS line
WHERE line = "$ cd .."
WITH "MATCH (new_cur:Directory)-[:CONTAINS]->(cur:Directory:Current)
REMOVE cur:Current
SET new_cur:Current;" AS cypherCode, n, line AS bashCode
CREATE (:TerminalLine {n:n, cypherCode: cypherCode, bashCode: bashCode});

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH linenumber() AS n, line[0] AS line
WHERE line STARTS WITH '$ cd ' AND NOT line IN ["$ cd /", "cd .."]
WITH substring(line, 5) AS name, n, line AS bashCode
WITH "MATCH (cur:Directory:Current)-[:CONTAINS]->(new_cur:Directory {name: '"+name+"'})
REMOVE cur:Current
SET new_cur:Current;" AS cypherCode, n, bashCode
CREATE (:TerminalLine {n:n, cypherCode: cypherCode, bashCode: bashCode});

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH linenumber() AS n, line[0] AS line
WHERE line STARTS WITH 'dir '
WITH substring(line, 4) AS name, n, line AS bashCode
WITH "MATCH (cur:Directory:Current)
MERGE (new:Directory {name:'"+name+"', path: cur.path+'"+name+"/'})
MERGE (cur)-[:CONTAINS]->(new);" AS cypherCode, n, bashCode
CREATE (:TerminalLine {n:n, cypherCode: cypherCode, bashCode: bashCode});

LOAD CSV FROM 'file:///'+$env+'.txt' AS line
WITH linenumber() AS n, line[0] AS bash_line
WHERE bash_line=~ "[0-9].*"
WITH split(bash_line," ") AS line, n, bash_line
WITH toInteger(line[0]) AS fileSize, line[1] AS name, n, bash_line
WITH "MATCH (cur:Directory:Current)
MERGE (new:File {name:'"+name+"', path: cur.path+'"+name+"', size:"+fileSize+"})
MERGE (cur)-[:CONTAINS]->(new);" AS cypherCode, n, bash_line AS bashCode
CREATE (:TerminalLine {n:n, cypherCode: cypherCode, bashCode: bashCode});

MATCH (l:TerminalLine)
WITH l.n AS n, l
ORDER BY n
WITH collect(l) AS lines
CALL apoc.nodes.link(lines, "NEXT");

MATCH (tl:TerminalLine)
WITH tl, tl.n AS n
ORDER BY n
WITH collect(tl.cypherCode) AS cypherLines
WITH apoc.text.join(cypherLines, "\n") AS script
CALL apoc.cypher.runMany(
  script,
  {}
) YIELD result
RETURN result;

CALL apoc.periodic.commit("MATCH(d:Directory)-[:CONTAINS]->(child)
WHERE d.size IS null
WITH d, collect(child) AS children
WHERE none(child IN children WHERE child.size IS NULL)
UNWIND children AS child
WITH d, child.size AS child_size
WITH d, sum(child_size) AS dir_size
SET d.size = dir_size
WITH count(d) AS limit
RETURN limit", {});

MATCH (d:Directory WHERE d.size <= 100000)
RETURN sum (d.size) AS `part 1`;
