"""
Single entry point for SQL parsing using pglast.
Consolidates all parsing logic from extract.py, catalog.py, compare.py, and diff.py.
"""

import hashlib
import re
from typing import List, Optional, Union, Dict, Any
from pglast import parse_sql
from pg_compose_core.lib.ast_objects import ASTObject, BuildStage
from pg_compose_core.lib.ast_list import ASTList

# Constants
POSTGRES_BUILTINS = {
    'information_schema', 'pg_catalog', 'pg_toast', 'pg_temp', 'pg_toast_temp'
}

def normalize_sql(sql: str) -> str:
    """Normalize SQL for consistent hashing."""
    # Remove comments
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    # Remove extra whitespace
    sql = re.sub(r'\s+', ' ', sql)
    return sql.strip()

def extract_schema_info(rel_node) -> tuple[Optional[str], Optional[str]]:
    """Extract schema and table name from a relation node."""
    if not rel_node:
        return None, None
    
    schema = None
    table_name = None
    
    if hasattr(rel_node, "schemaname") and rel_node.schemaname:
        schema = rel_node.schemaname.lower()
    
    if hasattr(rel_node, "relname") and rel_node.relname:
        table_name = rel_node.relname.lower()
    
    return schema, table_name

def extract_qualified_name(rel_node) -> str:
    """Extract fully qualified name (schema.table) from a relation node."""
    schema, table_name = extract_schema_info(rel_node)
    if schema and table_name:
        return f"{schema}.{table_name}"
    elif table_name:
        return table_name
    else:
        return None

def parse_sql_to_ast_objects(sql: str, grants: bool = True) -> ASTList:
    """
    Parse SQL string and return ASTList of ASTObjects.
    This is the single entry point for all SQL parsing.
    """
    try:
        raw_stmts = parse_sql(sql)
    except Exception as e:
        raise ValueError(f"Failed to parse SQL: {str(e)}")
    
    ast_objects = []
    
    for raw_stmt in raw_stmts:
        node = raw_stmt.stmt
        typename = type(node).__name__
        
        # Extract SQL slice and create normalized hash
        start = raw_stmt.stmt_location
        end = start + raw_stmt.stmt_len
        query_text = sql[start:end]
        normalized_sql = normalize_sql(query_text)
        query_hash = hashlib.sha256(normalized_sql.encode()).hexdigest()
        
        # Parse based on statement type
        if typename == "CreateStmt":
            ast_obj = _parse_create_statement(node, query_text, query_hash, start, end)
            if ast_obj:
                ast_objects.append(ast_obj)
        
        elif typename == "IndexStmt":
            ast_obj = _parse_index_statement(node, query_text, query_hash, start, end)
            if ast_obj:
                ast_objects.append(ast_obj)
        
        elif typename == "AlterTableStmt":
            ast_obj = _parse_alter_table_statement(node, query_text, query_hash, start, end)
            if ast_obj:
                ast_objects.append(ast_obj)
        
        elif typename == "CreatePolicyStmt":
            ast_obj = _parse_policy_statement(node, query_text, query_hash, start, end)
            if ast_obj:
                ast_objects.append(ast_obj)
        
        elif typename == "GrantStmt" and grants:
            ast_objs = _parse_grant_statement(node, query_text, query_hash, start, end)
            ast_objects.extend(ast_objs)
        
        elif typename == "ViewStmt":
            ast_obj = _parse_view_statement(node, query_text, query_hash, start, end)
            if ast_obj:
                ast_objects.append(ast_obj)
        
        elif typename == "CreateFunctionStmt":
            ast_obj = _parse_function_statement(node, query_text, query_hash, start, end)
            if ast_obj:
                ast_objects.append(ast_obj)
        
        # Add more statement types as needed...
    
    return ASTList(ast_objects)

def _parse_create_statement(node, query_text: str, query_hash: str, start: int, end: int) -> Optional[ASTObject]:
    """Parse CREATE TABLE, CREATE VIEW, etc. statements."""
    rel = getattr(node, "relation", None)
    if not rel:
        return None
    
    schema, table_name = extract_schema_info(rel)
    
    # Determine object type
    if hasattr(node, "tableElts") and node.tableElts:
        query_type = BuildStage.BASE_TABLE
    elif hasattr(node, "viewQuery"):
        query_type = BuildStage.VIEW
    else:
        query_type = BuildStage.UNKNOWN
    
    # Extract dependencies
    dependencies = []
    if hasattr(node, "tableElts") and node.tableElts:
        for elt in node.tableElts:
            if hasattr(elt, "constraint") and elt.constraint:
                constraint = elt.constraint
                if hasattr(constraint, "contype") and constraint.contype == 2:  # FOREIGN KEY
                    if hasattr(constraint, "pktable") and constraint.pktable:
                        pk_schema, pk_table = extract_schema_info(constraint.pktable)
                        if pk_table:
                            dep_name = f"{pk_schema}.{pk_table}" if pk_schema else pk_table
                            dependencies.append(dep_name)
    
    return ASTObject(
        command=query_text,
        object_name=table_name,
        query_type=query_type,
        dependencies=dependencies,
        query_hash=query_hash,
        query_start_pos=start,
        query_end_pos=end,
        schema=schema,
        ast_node=node
    )

def _parse_index_statement(node, query_text: str, query_hash: str, start: int, end: int) -> Optional[ASTObject]:
    """Parse CREATE INDEX statements."""
    index_name = getattr(node, "idxname", None)
    rel = getattr(node, "relation", None)
    
    if not rel or not index_name:
        return None
    
    schema, table_name = extract_schema_info(rel)
    dependencies = []
    
    if table_name:
        dep_name = f"{schema}.{table_name}" if schema else table_name
        dependencies.append(dep_name)
    
    return ASTObject(
        command=query_text,
        object_name=index_name,
        query_type=BuildStage.INDEX,
        dependencies=dependencies,
        query_hash=query_hash,
        query_start_pos=start,
        query_end_pos=end,
        schema=schema,
        ast_node=node
    )

def _parse_alter_table_statement(node, query_text: str, query_hash: str, start: int, end: int) -> Optional[ASTObject]:
    """Parse ALTER TABLE statements (constraints, etc.)."""
    from pglast.enums import AlterTableType
    rel = getattr(node, "relation", None)
    if not rel:
        return None
    
    schema, table_name = extract_schema_info(rel)
    
    # Determine if this is a constraint
    cmds = getattr(node, "cmds", [])
    for cmd in cmds:
        if hasattr(cmd, "subtype") and cmd.subtype == AlterTableType.AT_AddConstraint:
            constraint = cmd.def_
            if hasattr(constraint, "conname"):
                return ASTObject(
                    command=query_text,
                    object_name=constraint.conname,
                    query_type=BuildStage.CONSTRAINT,
                    dependencies=[f"{schema}.{table_name}" if schema else table_name],
                    query_hash=query_hash,
                    query_start_pos=start,
                    query_end_pos=end,
                    schema=schema,
                    ast_node=node
                )
    
    return None

def _parse_policy_statement(node, query_text: str, query_hash: str, start: int, end: int) -> Optional[ASTObject]:
    """Parse CREATE POLICY statements."""
    rel = getattr(node, "table", None)
    policy_name = getattr(node, "policy_name", None)
    
    if not rel or not policy_name:
        return None
    
    schema, table_name = extract_schema_info(rel)
    dependencies = []
    
    if table_name:
        dep_name = f"{schema}.{table_name}" if schema else table_name
        dependencies.append(dep_name)
    
    return ASTObject(
        command=query_text,
        object_name=policy_name,
        query_type=BuildStage.POLICY,
        dependencies=dependencies,
        query_hash=query_hash,
        query_start_pos=start,
        query_end_pos=end,
        schema=schema,
        ast_node=node
    )

def _parse_grant_statement(node, query_text: str, query_hash: str, start: int, end: int) -> List[ASTObject]:
    """Parse GRANT statements."""
    from pg_compose_core.lib.ast_objects import ResourceType
    
    ast_objects = []
    objs = getattr(node, "objects", []) or []
    
    for obj in objs:
        dependencies = []
        object_name = None
        schema = None
        resource_type = ResourceType.UNKNOWN
        
        # Handle different object types in GRANT statements
        if hasattr(obj, "schemaname") and hasattr(obj, "relname") and obj.schemaname and obj.relname:
            schema = obj.schemaname.lower()
            table_name = obj.relname.lower()
            object_name = table_name
            qualified_name = f"{schema}.{table_name}"
            dependencies.append(qualified_name)
            resource_type = ResourceType.TABLE
        elif hasattr(obj, "relname") and obj.relname:
            table_name = obj.relname.lower()
            object_name = table_name
            dependencies.append(table_name)
            resource_type = ResourceType.TABLE
        elif hasattr(obj, "names") and obj.names:
            qualified_name = ".".join(str(n.sval).lower() for n in obj.names)
            object_name = qualified_name
            dependencies.append(qualified_name)
            resource_type = ResourceType.SCHEMA
        elif hasattr(obj, "objname") and obj.objname:
            if len(obj.objname) > 1:
                func_name = str(obj.objname[-1].sval).lower()
                object_name = func_name
                dependencies.append(func_name)
            else:
                func_name = str(obj.objname[0].sval).lower()
                object_name = func_name
                dependencies.append(func_name)
            resource_type = ResourceType.FUNCTION
        else:
            object_name = "unknown_resource"
        
        # Clean up dependency list
        filtered_deps = [
            d for d in dependencies
            if d not in POSTGRES_BUILTINS and not d.startswith("pg_")
        ]
        
        ast_objects.append(ASTObject(
            command=query_text,
            object_name=object_name,
            query_type=BuildStage.GRANT,
            resource_type=resource_type,
            dependencies=filtered_deps,
            query_hash=query_hash,
            query_start_pos=start,
            query_end_pos=end,
            schema=schema,
            ast_node=node
        ))
    
    return ast_objects

def _parse_view_statement(node, query_text: str, query_hash: str, start: int, end: int) -> Optional[ASTObject]:
    """Parse CREATE VIEW statements."""
    rel = getattr(node, "view", None)
    if not rel:
        return None
    
    schema, view_name = extract_schema_info(rel)
    
    # Extract dependencies from the view query
    dependencies = []
    view_query = getattr(node, "query", None)
    if view_query:
        # This is a simplified dependency extraction
        # In practice, you'd want to parse the view query to find table references
        pass
    
    return ASTObject(
        command=query_text,
        object_name=view_name,
        query_type=BuildStage.VIEW,
        dependencies=dependencies,
        query_hash=query_hash,
        query_start_pos=start,
        query_end_pos=end,
        schema=schema,
        ast_node=node
    )

def _parse_function_statement(node, query_text: str, query_hash: str, start: int, end: int) -> Optional[ASTObject]:
    """Parse CREATE FUNCTION statements."""
    func_name = None
    if hasattr(node, "funcname") and node.funcname:
        if len(node.funcname) > 1:
            func_name = str(node.funcname[-1].sval).lower()
        else:
            func_name = str(node.funcname[0].sval).lower()
    
    if not func_name:
        return None
    
    return ASTObject(
        command=query_text,
        object_name=func_name,
        query_type=BuildStage.FUNCTION,
        dependencies=[],
        query_hash=query_hash,
        query_start_pos=start,
        query_end_pos=end,
        schema=None,  # Could extract from funcname if needed
        ast_node=node
    )

# Legacy compatibility functions
def extract_build_queries(sql: str, use_ast_objects: bool = True, grants: bool = True) -> Union[ASTList, List[dict]]:
    """Legacy function for backward compatibility."""
    if use_ast_objects:
        return parse_sql_to_ast_objects(sql, grants=grants)
    else:
        # Convert ASTObjects to dict format for legacy compatibility
        ast_objects = parse_sql_to_ast_objects(sql, grants=grants)
        return [obj.__dict__ for obj in ast_objects]

def load_source(source: str, schemas: Optional[List[str]] = None, grants: bool = True) -> ASTList:
    """Load schema objects from a source (file, directory, or connection string)."""
    import os
    
    # Handle .sql files
    if source.endswith('.sql'):
        with open(source, 'r') as f:
            sql = f.read()
        return parse_sql_to_ast_objects(sql, grants=grants)
    
    # Handle git repositories
    elif source.startswith(('git@', 'https://')) and ('.git' in source):
        from pg_compose_core.lib.git import extract_from_git_repo
        
        # Parse the source to separate repo URL from target path
        # The GitRepoContext handles #branch parsing internally
        repo_url = source
        target_path = None
        
        # Check if there's a path after .git/
        if '.git/' in source:
            # Split on .git/ to separate repo URL from path
            # But preserve any #branch suffix in the repo_url
            parts = source.split('.git/', 1)
            repo_url = parts[0] + '.git'
            target_path = parts[1]
        
        with extract_from_git_repo(repo_url, target_path) as working_dir:
            # Load all .sql files in the directory
            all_sql = []
            for root, dirs, files in os.walk(working_dir):
                for file in files:
                    if file.endswith('.sql'):
                        file_path = os.path.join(root, file)
                        with open(file_path, 'r') as f:
                            all_sql.append(f.read())
            
            if not all_sql:
                raise ValueError(f"No .sql files found in git repository: {source}")
            
            # Parse all SQL files
            combined_sql = '\n\n'.join(all_sql)
            return parse_sql_to_ast_objects(combined_sql, grants=grants)
    
    # Handle postgres:// URIs
    elif source.startswith('postgres://') or source.startswith('postgresql://'):
        # TODO: Implement database connection logic
        raise NotImplementedError("Database connections not yet implemented")
    
    # Handle raw SQL strings (if it contains SQL keywords, assume it's raw SQL)
    elif any(keyword in source.upper() for keyword in ['CREATE', 'DROP', 'ALTER', 'GRANT', 'REVOKE', 'SELECT', 'INSERT', 'UPDATE', 'DELETE']):
        return parse_sql_to_ast_objects(source, grants=grants)
    
    # Handle directories (look for .sql files)
    elif os.path.isdir(source):
        all_sql = []
        for root, dirs, files in os.walk(source):
            for file in files:
                if file.endswith('.sql'):
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r') as f:
                        all_sql.append(f.read())
        
        if not all_sql:
            raise ValueError(f"No .sql files found in directory: {source}")
        
        # Parse all SQL files
        combined_sql = '\n\n'.join(all_sql)
        return parse_sql_to_ast_objects(combined_sql, grants=grants)
    
    else:
        raise NotImplementedError(f"Source type not supported: {source}") 