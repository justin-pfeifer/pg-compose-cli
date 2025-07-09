from typing import List, Dict, Optional
from collections import defaultdict, deque

def sort_queries(queries: List[dict], use_object_names: bool = False, grant_handling: bool = False) -> List[dict]:
    """
    Sort queries by dependencies using topological sort.
    
    Args:
        queries: List of query objects with dependencies
        use_object_names: If True, use object_name as primary key instead of query_hash
        grant_handling: If True, apply special logic for GRANT dependencies
    """
    if use_object_names:
        return _sort_by_object_names(queries, grant_handling)
    else:
        return _sort_by_query_hash(queries)

def _sort_by_query_hash(queries: List[dict]) -> List[dict]:
    """Original sorting logic using query_hash as primary key."""
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

def _sort_by_object_names(queries: List[dict], grant_handling: bool = False) -> List[dict]:
    """Sorting logic using object_name as primary key with optional GRANT handling."""
    # Build object_name -> query map
    name_to_query = {}
    for q in queries:
        object_name = q.get("object_name")
        if object_name:
            name_to_query[object_name] = q

    # Build dependency -> object_name mapping for GRANTs (if enabled)
    dep_to_object = {}
    if grant_handling:
        for q in queries:
            object_name = q.get("object_name")
            if object_name and object_name.startswith("grant_on_"):
                # For GRANTs, map the dependency to the object name
                for dep in q.get("dependencies", []):
                    dep_to_object[dep] = object_name

    # Build graph using object names
    graph = defaultdict(set)
    in_degree = defaultdict(int)

    for q in queries:
        object_name = q.get("object_name")
        if object_name:
            for dep in q.get("dependencies", []):
                # Check if dependency exists as an object name
                if dep in name_to_query:
                    graph[dep].add(object_name)
                    in_degree[object_name] += 1
                # Check if dependency maps to a GRANT object name (if grant handling enabled)
                elif grant_handling and dep in dep_to_object:
                    grant_obj_name = dep_to_object[dep]
                    if grant_obj_name in name_to_query and grant_obj_name != object_name:
                        graph[grant_obj_name].add(object_name)
                        in_degree[object_name] += 1
                # If dependency doesn't exist in query objects, ignore it
                # (it means the object is already created or doesn't need to be created)

    # Topological sort
    queue = deque([name for name in name_to_query if in_degree[name] == 0])
    sorted_names = []

    while queue:
        current = queue.popleft()
        sorted_names.append(current)
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(sorted_names) != len(name_to_query):
        raise ValueError("Cyclic dependency detected")

    # Return queries in sorted order
    sorted_queries = []
    
    # First, add queries that have object names and dependencies (in sorted order)
    for name in sorted_names:
        sorted_queries.append(name_to_query[name])
    
    # Then, add queries that have object names but no dependencies (not in sorted_names)
    for q in queries:
        object_name = q.get("object_name")
        if object_name and object_name not in sorted_names:
            sorted_queries.append(q)
    
    # Finally, add any queries without object names at the end
    for q in queries:
        if not q.get("object_name"):
            sorted_queries.append(q)
    
    return sorted_queries

def sort_alter_commands(command_objects: List[Dict]) -> List[Dict]:
    """
    Sort alter commands by dependencies using object names with GRANT handling.
    This is a convenience function that calls sort_queries with the appropriate flags.
    """
    return sort_queries(command_objects, use_object_names=True, grant_handling=True)
