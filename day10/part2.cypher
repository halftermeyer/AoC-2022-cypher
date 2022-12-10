:param wide => 40;
:param high => 6;

MATCH (i:Instr)
SET i.sprite = [i.X - 1, i.X, i.X + 1];

UNWIND range(1, $high * $wide) AS cycle
WITH cycle, cycle - 1 AS pix
WITH cycle, pix, pix / $wide AS row, pix % $wide AS col
MATCH (i:Instr WHERE i.from_cycle <= cycle <= i.to_cycle)
WITH cycle, row, col, i.sprite AS sprite, i.id AS id, i.X AS X, i.addX AS addX
WITH cycle, row, col, col IN sprite AS lit
WITH row, collect(lit) AS row_pix
WITH row, [isLit IN row_pix | CASE isLit WHEN true THEN "#" ELSE " " END] AS row_pix
WITH row, apoc.text.join(row_pix, "") AS row_pix
WITH collect(row_pix) AS screen_pix
RETURN apoc.text.join(screen_pix, "\n") AS `part 2`;

\\ ###  #  #  ##   ##   ##  ###  #  # ####
\\ #  # #  # #  # #  # #  # #  # #  #    #
\\ ###  #  # #    #  # #    ###  #  #   #
\\ #  # #  # #    #### #    #  # #  #  #
\\ #  # #  # #  # #  # #  # #  # #  # #
\\ ###   ##   ##  #  #  ##  ###   ##  #### 
