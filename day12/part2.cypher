MATCH (source:Square WHERE source.height = 'a'), (target:Square&Destination)
CALL gds.shortestPath.dijkstra.stream('height_map', {
    sourceNode: source,
    targetNode: target
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN min(toInteger(totalCost)) AS `part 2`;
