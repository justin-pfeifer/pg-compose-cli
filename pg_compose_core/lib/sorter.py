from typing import List, Dict, Optional, Union
from collections import defaultdict, deque
from pg_compose_core.lib.ast_objects import ASTObject

def sort_queries(objects: List[ASTObject], use_object_names: bool = True, grant_handling: bool = True) -> List[ASTObject]:
    """
    Sort queries by dependencies using topological sort.
    
    Args:
        queries: List of query objects with dependencies (dict or ASTObject)
        use_object_names: If True, use object_name as primary key instead of query_hash
        grant_handling: If True, apply special logic for GRANT dependencies
    """
    if use_object_names:
        return _sort_by_object_names(objects, grant_handling)
    else:
        return _sort_by_query_hash(objects)

def sort_alter_commands(command_objects: List[Union[Dict, ASTObject]]) -> List[Union[Dict, ASTObject]]:
    """
    Sort alter commands by dependencies using query_hash as the primary key.
    This is a convenience function that calls sort_queries with the appropriate flags.
    """
    return sort_queries(command_objects, use_object_names=False, grant_handling=True)

def _get_object_name(obj: Union[dict, ASTObject]) -> Optional[str]:
    """Extract object_name from various object types."""
    if isinstance(obj, ASTObject):
        return obj.object_name
    elif isinstance(obj, dict):
        return obj.get("object_name")
    return None

def _get_dependencies(obj: Union[dict, ASTObject]) -> List[str]:
    """Extract dependencies from various object types."""
    if isinstance(obj, ASTObject):
        return obj.dependencies
    elif isinstance(obj, dict):
        return obj.get("dependencies", [])
    return []

def _get_query_hash(obj: Union[dict, ASTObject]) -> Optional[str]:
    """Extract query_hash from various object types."""
    if isinstance(obj, ASTObject):
        return obj.query_hash
    elif isinstance(obj, dict):
        return obj.get("query_hash")
    return None

def _sort_by_query_hash(queries: List[ASTObject]) -> List[ASTObject]:
    """Original sorting logic using query_hash as primary key."""
    # Build object_name -> query_hash map
    name_to_hash = {}
    hash_to_query = {}

    for q in queries:
        object_name = _get_object_name(q)
        query_hash = _get_query_hash(q)
        
        if object_name and query_hash:
            name_to_hash[object_name] = query_hash
        if query_hash:
            hash_to_query[query_hash] = q

    # Build graph
    graph = defaultdict(set)
    in_degree = defaultdict(int)

    for q in queries:
        query_hash = _get_query_hash(q)
        if not query_hash:
            continue
            
        for dep in _get_dependencies(q):
            dep_hash = name_to_hash.get(dep)
            if dep_hash:
                graph[dep_hash].add(query_hash)
                in_degree[query_hash] += 1

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

    if len(sorted_hashes) != len(hash_to_query):
        raise ValueError("Cyclic dependency detected")

    return [hash_to_query[h] for h in sorted_hashes]

def _sort_by_object_names(queries: List[ASTObject], grant_handling: bool = False) -> List[ASTObject]:
    """Sorting logic using object_name as primary key with optional GRANT handling."""
    import logging
    logging.debug(f"DEBUG _sort_by_object_names input: {len(queries)} queries")
    
    # Build object_name -> query map
    name_to_query = {}
    for q in queries:
        object_name = _get_object_name(q)
        if object_name:
            name_to_query[object_name] = q
            logging.debug(f"DEBUG added to name_to_query: {object_name} -> {q}")
        else:
            logging.debug(f"DEBUG skipped object without name: {q}")
    
    logging.debug(f"DEBUG name_to_query has {len(name_to_query)} entries")
    
    # Build dependency -> object_name mapping for GRANTs (if enabled)
    dep_to_object = {}
    if grant_handling:
        for q in queries:
            object_name = _get_object_name(q)
            if object_name and object_name.startswith("grant_on_"):
                # For GRANTs, map the dependency to the object name
                for dep in _get_dependencies(q):
                    dep_to_object[dep] = object_name

    # Build graph using object names
    graph = defaultdict(set)
    in_degree = defaultdict(int)

    for q in queries:
        object_name = _get_object_name(q)
        if object_name:
            for dep in _get_dependencies(q):
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
        object_name = _get_object_name(q)
        if object_name and object_name not in sorted_names:
            sorted_queries.append(q)
    
    # Finally, add any queries without object names at the end
    for q in queries:
        if not _get_object_name(q):
            sorted_queries.append(q)
    
    logging.debug(f"DEBUG _sort_by_object_names output: {len(sorted_queries)} queries")
    return sorted_queries
