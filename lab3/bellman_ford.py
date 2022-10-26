"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Bobby Brown rbrown3
:Version: 1.0

Lab 3: Pub/Sub
Class: bellman_ford.py

Finds the shortest path and detects negative cycle path from a given graph 
using the Bellman-Ford algorithm.

Refernces:
    https://en.wikipedia.org/wiki/Bellman-Ford_algorithm

    https://www.baeldung.com/cs/bellman-ford
    
    https://www.geeksforgeeks.org/bellman-ford-algorithm-dp-23/
    
    Bellman-Ford Algorithm - Dynamic Programming Algorithms in Python (Part 3) 
    - https://www.youtube.com/watch?v=ne9eZ4ezg0Y

"""

FLOAT_REF = float('inf')    # Constant to hold infinity

class Bellman_Ford(object):
    """
    Bellman_Ford creates an object to perform the Bellman-Ford algorithm
    analysis on a passed in graph.
    """

    def __init__(self, graph) -> None:
        """
        Bellman_Ford object constructor

        Args:
            graph: The passed in graph used for the Bellman-Ford analysis
        """

        self.graph = graph
        self.vertices = len(graph)

    def shortest_paths(self, start_vertex, tolerance=0):
        """
        Finds the shortest paths within a graph, by determinning the sum of edge
        weights, from start_vertex to every other vertex. Additionally detects
        if there are negative cycles within the graph and reports one of them.

        For this function, tolerance is used for relaxation and cycle detection.
        Only relaxations resulting from an improvement greater than tolerance
        are considered. For negative cycle detection, if the sum of weights is
        greater than -tolerance, it is not reported as a negative cycle. This is
        useful when circuits are expected to be close to zero.

        Args:
            start_vertex: 
                The start of all the paths
            tolerance:
                Value to determine if a path needs to be relaxed
        
        Returns:
            distance:
                A dictionary keyed by vertex of shortest distance from 
                start_vertex to that vertex
            predecessor:
                A dictionary keyed by vertex of previous vertex in shortest path
                from start_vertex
            negative_cyle:
                None if no negative cycle found, otherwise an edge (u,v) in
                such a negative cycle
        """

        # Construct dictionaries from the graph by initializing the shortest
        # distance to infinity and predecessor vertex to None
        distance = {vertex: FLOAT_REF for vertex in self.graph}
        predecessor = {vertex: None for vertex in self.graph}
        
        # Set the shortest distance of the start_vertex to 0
        distance[start_vertex] = 0

        # Determine shortest path
        # Loop through the graph and relax all edges to find the shortest path
        # with at most vertices - 1 edges
        for _ in range(self.vertices - 1):
            # Update distance and precessor based on the adjacent vertices that
            # are next within the graph
            for current in self.graph:
                for next in self.graph[current]:
                    weight = self.graph[current][next]['price']
                    
                    if distance[current] is not FLOAT_REF and distance[current] + weight + tolerance < distance[next]:
                        distance[next] = distance[current] + weight
                        predecessor[next] = current
        
        # Negative cycle detection
        # Loop through the entire graph to look for negative distance values
        for current in self.graph:
            for next in self.graph[current]:
                weight = self.graph[current][next]['price']
            
                if distance[current] is not FLOAT_REF and distance[current] + weight + tolerance < distance[next]:
                    # Return the distance, predecessor dictionaires and the edge
                    # for the negative cycle
                    return distance, predecessor, (current, next)
        
        # Return distance and predecessor dictionaries, with None for edge
        return distance, predecessor, None