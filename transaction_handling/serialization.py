'''
author: Sarthak Khandelwal
'''

from collections import defaultdict, deque

class SerializationGraph:

    def __init__(self):
        self.graph = defaultdict(dict)
    
    def add_edge(self, from_tx, to_tx, edge_type):
        """
        Adds a edge between two transactions such that
                     edge_type
            from_tx -----------> to_tx           
        """
        if from_tx not in self.graph.keys():
            self.graph.update({from_tx: {}})
        if to_tx != '':
            if to_tx not in self.graph.keys():
                self.graph.update({to_tx: {}})
            try:
                self.graph[from_tx][to_tx].add(edge_type)
            except KeyError as e:
                self.graph[from_tx][to_tx] = set()
                self.graph[from_tx][to_tx].add(edge_type)

    def remove_transaction(self, tx):
        """
        Remove a transaction from the graph. Removes the node
        and all edges associated with it.
        """
        self.graph.pop(tx, None)
        for neighbours in self.graph.values():
            neighbours.pop(tx, None)
    
    def has_cycle_with_two_rw(self):
        """
        Detect if there is a cycle in the graph containing exactly two 
        consecutive `rw` edges in a row.
        """
        def dfs(tx, visited, stack, prev_edge_type, rw_edge_count):
            visited.add(tx)
            stack.add(tx)

            for neighbor, edge_type in self.graph[tx].items():
                if 'rw' in edge_type:
                    # Increment the count of consecutive `rw` edges if the previous edge was also `rw`
                    rw_edge_count = rw_edge_count + 1 if prev_edge_type == 'rw' else 1
                    edge_type = 'rw'

                # If there are two consecutive `rw` edges, a problematic cycle is detected
                if rw_edge_count >= 2 and neighbor in stack:
                    return True

                # Continue DFS if the neighbor hasn't been visited
                if neighbor not in visited:
                    if dfs(neighbor, visited, stack, edge_type, rw_edge_count):
                        return True

            # Backtrack: remove current transaction from the stack
            stack.remove(tx)
            return False

        visited = set()

        # Start DFS from every unvisited node
        for tx in self.graph:
            if tx not in visited:
                if dfs(tx, visited, set(), None, 0):  # Initialize with no previous edge and count 0
                    return True

        return False
            