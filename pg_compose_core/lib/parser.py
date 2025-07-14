"""
Single entry point for SQL parsing using pglast.
Consolidates all parsing logic from extract.py, catalog.py, compare.py, and diff.py.
"""

import hashlib
import re
from typing import List, Optional, Union, Dict, Any
from pglast import parse_sql, parse_plpgsql
from pg_compose_core.lib.ast.objects import ASTObject, BuildStage, ResourceType
from pg_compose_core.lib.ast.function import FunctionASTObject, FunctionParameter
from pg_compose_core.lib.ast.table import TableASTObject, TableColumn, TableConstraint
from pg_compose_core.lib.ast.list import ASTList

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
        # Try parse_plpgsql as fallback
        try:
            raw_stmts = parse_plpgsql(sql)
        except Exception as plpgsql_e:
            # If both fail, rethrow the original exception
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
            # Check if this is actually a procedure
            is_procedure = getattr(node, "is_procedure", False)
            if is_procedure:
                ast_obj = _parse_procedure_statement(node, query_text, query_hash, start, end)
            else:
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
    
    # For tables, create TableASTObject with column and constraint information
    if query_type == BuildStage.BASE_TABLE:
        columns = []
        constraints = []
        
        if hasattr(node, "tableElts") and node.tableElts:
            for elt in node.tableElts:
                if hasattr(elt, "colname") and hasattr(elt, "typeName"):
                    # This is a column definition
                    col_name = str(elt.colname)
                    col_type = _extract_full_type_name(elt.typeName)
                    is_nullable = not getattr(elt, "is_not_null", False)
                    default = None
                    
                    # Check for default value
                    if hasattr(elt, "raw_default") and elt.raw_default:
                        default = str(elt.raw_default)
                    
                    columns.append(TableColumn(
                        name=col_name,
                        data_type=col_type,
                        is_nullable=is_nullable,
                        default=default
                    ))
                
                elif hasattr(elt, "constraint") and elt.constraint:
                    # This is a table-level constraint
                    constraint = elt.constraint
                    constraint_name = getattr(constraint, "conname", None)
                    constraint_type = _get_constraint_type(constraint)
                    constraint_columns = _extract_constraint_columns(constraint)
                    
                    constraints.append(TableConstraint(
                        name=constraint_name,
                        constraint_type=constraint_type,
                        columns=constraint_columns
                    ))
        
        return TableASTObject(
            command=query_text,
            object_name=table_name,
            dependencies=dependencies,
            query_hash=query_hash,
            query_start_pos=start,
            query_end_pos=end,
            schema=schema,
            ast_node=node,
            columns=columns,
            constraints=constraints
        )
    
    # For non-table objects, return regular ASTObject
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
    from pg_compose_core.lib.ast import ResourceType
    
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
    """Parse CREATE FUNCTION statements including PL/pgSQL functions."""
    func_name = None
    schema = None
    
    # Extract function name and schema
    if hasattr(node, "funcname") and node.funcname:
        if len(node.funcname) > 1:
            # Function has schema qualification
            schema = str(node.funcname[0].sval).lower()
            func_name = str(node.funcname[-1].sval).lower()
        else:
            # Function without schema (uses search_path)
            func_name = str(node.funcname[0].sval).lower()
    
    if not func_name:
        return None
    
    # Extract function parameters
    parameters = []
    if hasattr(node, "parameters") and node.parameters:
        for param in node.parameters:
            param_name = getattr(param, "name", None)
            param_type = getattr(param, "argType", None)
            param_mode = getattr(param, "mode", None)
            param_default = getattr(param, "defexpr", None)
            
            if param_name and param_type:
                # Skip table columns (parameters with mode 't' are TABLE return columns)
                if param_mode is not None:
                    mode_value = None
                    if hasattr(param_mode, 'value'):
                        mode_value = param_mode.value
                    elif hasattr(param_mode, 'name'):
                        mode_value = param_mode.name
                    else:
                        mode_value = str(param_mode)
                    
                    # Skip table columns (mode 't')
                    if mode_value == 't':
                        continue
                
                # Extract full type information including precision/scale
                type_name = _extract_full_type_name(param_type)
                
                # Convert parameter mode to string representation
                mode_str = None
                if param_mode is not None:
                    # param_mode is an enum, get the value (e.g., 'd' for default, 'i' for in, etc.)
                    if hasattr(param_mode, 'value'):
                        mode_value = param_mode.value
                        # Only include mode if it's not the default ('d')
                        if mode_value != 'd':
                            mode_str = mode_value
                    elif hasattr(param_mode, 'name'):
                        # Fallback to name if value not available
                        mode_str = param_mode.name
                    else:
                        mode_str = str(param_mode)
                
                parameters.append(FunctionParameter(
                    name=str(param_name),  # param_name is already a string
                    data_type=type_name,
                    mode=mode_str,
                    default_value=str(param_default) if param_default else None
                ))
    
    # Extract return type
    return_type = None
    if hasattr(node, "returnType") and node.returnType:
        return_type = _extract_full_type_name(node.returnType)
    
    # Extract function options (language, volatility, etc.)
    language = None
    volatility = None
    security = None
    is_aggregate = False
    is_window = False
    is_leakproof = False
    parallel = None
    
    if hasattr(node, "options") and node.options:
        for option in node.options:
            if hasattr(option, "defname"):
                option_name = option.defname
                option_value = getattr(option, "arg", None)
                
                if option_name == "language":
                    language = str(option_value.sval) if hasattr(option_value, "sval") else str(option_value)
                elif option_name == "volatility":
                    volatility = str(option_value.sval) if hasattr(option_value, "sval") else str(option_value)
                elif option_name == "security":
                    security = str(option_value.sval) if hasattr(option_value, "sval") else str(option_value)
                elif option_name == "parallel":
                    parallel = str(option_value.sval) if hasattr(option_value, "sval") else str(option_value)
                elif option_name == "leakproof":
                    is_leakproof = True
                elif option_name == "aggregate":
                    is_aggregate = True
                elif option_name == "window":
                    is_window = True
    
    # Extract dependencies from function body
    dependencies = []
    
    # Get function body
    func_body = None
    if hasattr(node, "options") and node.options:
        for option in node.options:
            if hasattr(option, "defname") and option.defname == "as":
                if hasattr(option, "arg") and option.arg:
                    if hasattr(option.arg, "sval"):
                        func_body = option.arg.sval
                    elif isinstance(option.arg, list):
                        # Handle list of strings (multi-line function body)
                        func_body = " ".join(str(arg.sval) for arg in option.arg if hasattr(arg, "sval"))
    
    # Extract table dependencies from function body using proper SQL parsing
    if func_body:
        dependencies = _extract_function_dependencies_with_parser(func_body)
    
    return FunctionASTObject(
        command=query_text,
        object_name=func_name,
        dependencies=dependencies,
        query_hash=query_hash,
        query_start_pos=start,
        query_end_pos=end,
        schema=schema,
        ast_node=node,
        parameters=parameters,
        return_type=return_type,
        language=language,
        volatility=volatility,
        security=security,
        is_aggregate=is_aggregate,
        is_window=is_window,
        is_leakproof=is_leakproof,
        parallel=parallel,
        function_body=func_body
    )

def _parse_procedure_statement(node, query_text: str, query_hash: str, start: int, end: int) -> Optional[ASTObject]:
    """Parse CREATE PROCEDURE statements."""
    proc_name = None
    schema = None
    
    # Extract procedure name and schema
    if hasattr(node, "funcname") and node.funcname:
        if len(node.funcname) > 1:
            # Procedure has schema qualification
            schema = str(node.funcname[0].sval).lower()
            proc_name = str(node.funcname[-1].sval).lower()
        else:
            # Procedure without schema (uses search_path)
            proc_name = str(node.funcname[0].sval).lower()
    
    if not proc_name:
        return None
    
    # Extract procedure parameters (same as functions)
    parameters = []
    if hasattr(node, "parameters") and node.parameters:
        for param in node.parameters:
            param_name = getattr(param, "name", None)
            param_type = getattr(param, "argType", None)
            param_mode = getattr(param, "mode", None)
            param_default = getattr(param, "defexpr", None)
            
            if param_name and param_type:
                # Skip table columns (parameters with mode 't' are TABLE return columns)
                if param_mode is not None:
                    mode_value = None
                    if hasattr(param_mode, 'value'):
                        mode_value = param_mode.value
                    elif hasattr(param_mode, 'name'):
                        mode_value = param_mode.name
                    else:
                        mode_value = str(param_mode)
                    
                    # Skip table columns (mode 't')
                    if mode_value == 't':
                        continue
                
                # Extract full type information including precision/scale
                type_name = _extract_full_type_name(param_type)
                
                # Convert parameter mode to string representation
                mode_str = None
                if param_mode is not None:
                    if hasattr(param_mode, 'value'):
                        mode_value = param_mode.value
                        if mode_value != 'd':
                            mode_str = mode_value
                    elif hasattr(param_mode, 'name'):
                        mode_str = param_mode.name
                    else:
                        mode_str = str(param_mode)
                
                parameters.append(FunctionParameter(
                    name=str(param_name),
                    data_type=type_name,
                    mode=mode_str,
                    default_value=str(param_default) if param_default else None
                ))
    
    # Extract procedure options (language, volatility, etc.)
    language = None
    volatility = None
    security = None
    is_aggregate = False
    is_window = False
    is_leakproof = False
    parallel = None
    
    if hasattr(node, "options") and node.options:
        for option in node.options:
            if hasattr(option, "defname"):
                option_name = option.defname
                option_value = getattr(option, "arg", None)
                
                if option_name == "language":
                    language = str(option_value.sval) if hasattr(option_value, "sval") else str(option_value)
                elif option_name == "volatility":
                    volatility = str(option_value.sval) if hasattr(option_value, "sval") else str(option_value)
                elif option_name == "security":
                    security = str(option_value.sval) if hasattr(option_value, "sval") else str(option_value)
                elif option_name == "parallel":
                    parallel = str(option_value.sval) if hasattr(option_value, "sval") else str(option_value)
                elif option_name == "leakproof":
                    is_leakproof = True
                elif option_name == "aggregate":
                    is_aggregate = True
                elif option_name == "window":
                    is_window = True
    
    # Extract dependencies from procedure body
    dependencies = []
    
    # Get procedure body
    proc_body = None
    if hasattr(node, "options") and node.options:
        for option in node.options:
            if hasattr(option, "defname") and option.defname == "as":
                if hasattr(option, "arg") and option.arg:
                    if hasattr(option.arg, "sval"):
                        proc_body = option.arg.sval
                    elif isinstance(option.arg, list):
                        # Handle list of strings (multi-line procedure body)
                        proc_body = " ".join(str(arg.sval) for arg in option.arg if hasattr(arg, "sval"))
    
    # Extract table dependencies from procedure body using proper SQL parsing
    if proc_body:
        dependencies = _extract_function_dependencies_with_parser(proc_body)
    
    return FunctionASTObject(
        command=query_text,
        object_name=proc_name,
        dependencies=dependencies,
        query_hash=query_hash,
        query_start_pos=start,
        query_end_pos=end,
        schema=schema,
        ast_node=node,
        parameters=parameters,
        return_type=None,  # Procedures don't have return types
        language=language,
        volatility=volatility,
        security=security,
        is_aggregate=is_aggregate,
        is_window=is_window,
        is_leakproof=is_leakproof,
        parallel=parallel,
        function_body=proc_body,
        query_type=BuildStage.PROCEDURE  # Override the default FUNCTION type
    )

def _extract_function_dependencies_with_parser(func_body: str) -> List[str]:
    """
    Extract table dependencies from PL/pgSQL function body using proper SQL parsing.
    Uses parse_sql with parse_plpgsql fallback to find table references.
    """
    dependencies = []
    
    try:
        # Try to parse the function body as SQL first
        stmts = parse_sql(func_body)
        for stmt in stmts:
            deps = _extract_dependencies_from_ast_node(stmt.stmt)
            dependencies.extend(deps)
    except Exception:
        # If parse_sql fails, try parse_plpgsql
        try:
            stmts = parse_plpgsql(func_body)
            for stmt in stmts:
                # parse_plpgsql returns dicts, not objects with .stmt
                deps = _extract_dependencies_from_plpgsql_stmt(stmt)
                dependencies.extend(deps)
        except Exception:
            # If both parsers fail, return empty dependencies
            pass
    
    # Remove duplicates while preserving order
    seen = set()
    unique_deps = []
    for dep in dependencies:
        if dep not in seen:
            seen.add(dep)
            unique_deps.append(dep)
    
    return unique_deps

def _extract_dependencies_from_ast_node(node) -> List[str]:
    """Extract table dependencies from a pglast AST node."""
    dependencies = []
    
    if not node:
        return dependencies
    
    node_type = type(node).__name__
    
    if node_type == "SelectStmt":
        # Handle SELECT statements
        if hasattr(node, "fromClause") and node.fromClause:
            for from_item in node.fromClause:
                if hasattr(from_item, "relname"):
                    table_name = from_item.relname.lower()
                    schema = getattr(from_item, "schemaname", None)
                    if schema:
                        qualified_name = f"{schema.lower()}.{table_name}"
                    else:
                        qualified_name = table_name
                    
                    if qualified_name.lower() not in POSTGRES_BUILTINS:
                        dependencies.append(qualified_name)
    
    elif node_type == "UpdateStmt":
        # Handle UPDATE statements
        if hasattr(node, "relation") and node.relation:
            table_name = node.relation.relname.lower()
            schema = getattr(node.relation, "schemaname", None)
            if schema:
                qualified_name = f"{schema.lower()}.{table_name}"
            else:
                qualified_name = table_name
            
            if qualified_name.lower() not in POSTGRES_BUILTINS:
                dependencies.append(qualified_name)
    
    elif node_type == "DeleteStmt":
        # Handle DELETE statements
        if hasattr(node, "relation") and node.relation:
            table_name = node.relation.relname.lower()
            schema = getattr(node.relation, "schemaname", None)
            if schema:
                qualified_name = f"{schema.lower()}.{table_name}"
            else:
                qualified_name = table_name
            
            if qualified_name.lower() not in POSTGRES_BUILTINS:
                dependencies.append(qualified_name)
    
    # Recursively check child nodes
    for attr_name in dir(node):
        if not attr_name.startswith('_'):
            attr_value = getattr(node, attr_name)
            if isinstance(attr_value, list):
                for item in attr_value:
                    if hasattr(item, '__class__'):
                        deps = _extract_dependencies_from_ast_node(item)
                        dependencies.extend(deps)
            elif hasattr(attr_value, '__class__'):
                deps = _extract_dependencies_from_ast_node(attr_value)
                dependencies.extend(deps)
    
    return dependencies

def _extract_full_type_name(type_node) -> str:
    """Extract full type name including precision, scale, and other modifiers."""
    if not type_node:
        return "unknown"
    
    # Get the base type name
    if hasattr(type_node, "names") and type_node.names:
        base_type = str(type_node.names[-1].sval)
    else:
        base_type = str(type_node)
    
    # Handle type modifiers (precision, scale, etc.)
    modifiers = []
    
    if hasattr(type_node, "typmods") and type_node.typmods:
        for typmod in type_node.typmods:
            # typmod is A_Const with val containing Integer
            if hasattr(typmod, "val") and hasattr(typmod.val, "ival"):
                modifiers.append(str(typmod.val.ival))
            elif hasattr(typmod, "ival"):
                modifiers.append(str(typmod.ival))
            elif hasattr(typmod, "sval"):
                modifiers.append(str(typmod.sval))
            else:
                modifiers.append(str(typmod))
    
    # Build the full type name
    if modifiers:
        return f"{base_type}({', '.join(modifiers)})"
    else:
        return base_type

def _extract_dependencies_from_plpgsql_stmt(stmt_dict: dict) -> List[str]:
    """Extract table dependencies from a parse_plpgsql statement dict."""
    dependencies = []
    
    # This is a simplified implementation for parse_plpgsql output
    # The actual structure depends on what parse_plpgsql returns
    # For now, we'll do a basic text search as fallback
    import re
    
    stmt_text = str(stmt_dict)
    
    # Look for table references in the statement
    from_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.?[a-zA-Z_][a-zA-Z0-9_]*)\b'
    matches = re.findall(from_pattern, stmt_text, re.IGNORECASE)
    
    for match in matches:
        table_name = match.strip()
        if table_name and table_name.lower() not in POSTGRES_BUILTINS:
            dependencies.append(table_name.lower())
    
    return dependencies

def _get_constraint_type(constraint) -> str:
    """Extract constraint type from constraint node."""
    if hasattr(constraint, "contype"):
        contype = constraint.contype
        if contype == 1:
            return "NOT NULL"
        elif contype == 2:
            return "FOREIGN KEY"
        elif contype == 3:
            return "PRIMARY KEY"
        elif contype == 4:
            return "UNIQUE"
        elif contype == 5:
            return "CHECK"
        elif contype == 6:
            return "EXCLUSION"
    return "UNKNOWN"

def _extract_constraint_columns(constraint) -> List[str]:
    """Extract column names from constraint node."""
    columns = []
    if hasattr(constraint, "keys") and constraint.keys:
        # This is for constraints that reference columns by index
        # We'd need to map these back to column names from the table definition
        # For now, return empty list - this would need more complex logic
        pass
    elif hasattr(constraint, "exclusions") and constraint.exclusions:
        # For exclusion constraints
        for exclusion in constraint.exclusions:
            if hasattr(exclusion, "name"):
                columns.append(str(exclusion.name))
    return columns

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
    
    # Handle git repositories first (before .sql files to avoid conflicts)
    if source.startswith(('git@', 'https://')) and ('.git' in source):
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
            # If target_path points to a specific .sql file, load just that file
            if target_path and target_path.endswith('.sql'):
                # For files, the working_dir is the repository root, so we need to join with target_path
                file_path = os.path.join(working_dir, target_path)
                if not os.path.exists(file_path):
                    raise ValueError(f"File '{target_path}' not found in git repository: {source}")
                with open(file_path, 'r') as f:
                    sql = f.read()
                return parse_sql_to_ast_objects(sql, grants=grants)
            else:
                # Load all .sql files in the directory
                all_sql = []
                search_dir = working_dir
                if target_path:
                    search_dir = os.path.join(working_dir, target_path)
                
                for root, dirs, files in os.walk(search_dir):
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
    
    # Handle .sql files
    elif source.endswith('.sql'):
        with open(source, 'r') as f:
            sql = f.read()
        return parse_sql_to_ast_objects(sql, grants=grants)
    
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