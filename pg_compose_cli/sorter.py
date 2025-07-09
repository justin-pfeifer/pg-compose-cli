from typing import List, Dict
from collections import defaultdict, deque

def sort_queries(queries: List[dict]) -> List[dict]:
    # Build object_name -> query_hash map
    name_to_hash = {}
    hash_to_query = {}

    for q in queries:
        object_name = q.get("object_name")
        if object_name:
            name_to_hash[object_name] = q["query_hash"]
        hash_to_query[q["query_hash"]] = q

    # Build graph
    graph = defaultdict(set)
    in_degree = defaultdict(int)

    for q in queries:
        this_hash = q["query_hash"]
        for dep in q["dependencies"]:
            dep_hash = name_to_hash.get(dep)
            if dep_hash:
                graph[dep_hash].add(this_hash)
                in_degree[this_hash] += 1

    # Topological sort
    queue = deque([h for h in hash_to_query if in_degree[h] == 0])
    sorted_hashes = []

    while queue:
        current = queue.popleft()
        sorted_hashes.append(current)
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(sorted_hashes) != len(queries):
        raise ValueError("Cyclic dependency detected")

    return [hash_to_query[h] for h in sorted_hashes]
