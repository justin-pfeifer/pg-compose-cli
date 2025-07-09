from typing import List, Dict

def generate_alter_commands(diff_result: Dict) -> List[str]:
    """Generate ALTER commands from a schema diff result."""
    commands = []
    
    # Handle dropped objects
    for obj in diff_result.get("dropped", []):
        if obj["query_type"] == "base_table":
            commands.append(f"DROP TABLE {obj['object_name']};")
        elif obj["query_type"] == "view":
            commands.append(f"DROP VIEW {obj['object_name']};")
        elif obj["query_type"] == "materialized_view":
            commands.append(f"DROP MATERIALIZED VIEW {obj['object_name']};")
        elif obj["query_type"] == "function":
            commands.append(f"DROP FUNCTION {obj['object_name']};")
        elif obj["query_type"] == "index":
            commands.append(f"DROP INDEX {obj['object_name']};")
    
    # Handle created objects
    for obj in diff_result.get("created", []):
        if obj["query_type"] == "base_table":
            table_def = obj.get("table_definition", _extract_table_definition(obj))
            commands.append(f"CREATE TABLE {obj['object_name']} (\n{table_def}\n);")
        elif obj["query_type"] == "view":
            # For views, use CREATE OR REPLACE VIEW
            original_sql = obj.get("view_definition", obj["query_text"])
            or_replace_sql = _replace_create_view_with_or_replace(original_sql)
            commands.append(_ensure_semicolon(or_replace_sql))
        elif obj["query_type"] == "materialized_view":
            # For materialized views, just use the original CREATE MATERIALIZED VIEW statement
            original_sql = obj.get("view_definition", obj["query_text"])
            commands.append(_ensure_semicolon(original_sql))
        elif obj["query_type"] == "function":
            commands.append(f"CREATE FUNCTION {obj['object_name']} {_extract_function_definition(obj)};")
        elif obj["query_type"] == "index":
            commands.append(f"CREATE INDEX {obj['object_name']} {_extract_index_definition(obj)};")
    
    # Handle changed objects
    for obj in diff_result.get("changed", []):
        if obj["query_type"] == "base_table":
            commands.extend(_generate_table_alter_commands(obj))
        elif obj["query_type"] in ["view", "materialized_view"]:
            commands.extend(_generate_view_alter_commands(obj))
        elif obj["query_type"] == "function":
            commands.extend(_generate_function_alter_commands(obj))
    
    return commands

def _generate_table_alter_commands(obj: Dict) -> List[str]:
    """Generate ALTER TABLE commands for table changes."""
    commands = []
    ast_diff = obj.get("ast_diff", {})
    
    # Add columns
    for column_def in ast_diff.get("add_columns", []):
        commands.append(f"ALTER TABLE {obj['object_name']} ADD COLUMN {column_def};")
    
    # Drop columns
    for column in ast_diff.get("drop_columns", []):
        commands.append(f"ALTER TABLE {obj['object_name']} DROP COLUMN {column};")
    
    # Change columns
    for change in ast_diff.get("change_columns", []):
        column_name = change["column"]
        
        if change.get("type"):
            old_type = change["type"]["from"]
            new_type = change["type"]["to"]
            commands.append(f"ALTER TABLE {obj['object_name']} ALTER COLUMN {column_name} TYPE {new_type};")
        
        if change.get("nullable") is not None:
            is_nullable = change["nullable"]["to"]
            constraint = "DROP NOT NULL" if is_nullable else "SET NOT NULL"
            commands.append(f"ALTER TABLE {obj['object_name']} ALTER COLUMN {column_name} {constraint};")
        
        if change.get("default") is not None:
            new_default = change["default"]["to"]
            if new_default is None:
                commands.append(f"ALTER TABLE {obj['object_name']} ALTER COLUMN {column_name} DROP DEFAULT;")
            else:
                commands.append(f"ALTER TABLE {obj['object_name']} ALTER COLUMN {column_name} SET DEFAULT {new_default};")
    
    return commands



def _extract_column_definition(obj: Dict, column_name: str) -> str:
    """Extract column definition from pre-processed schema data."""
    # For created objects, we can use the query_text directly
    if "query_text" in obj:
        # This is a simplified approach - in a real implementation,
        # you might want to extract the specific column from the CREATE TABLE
        return f"{column_name} TEXT"  # Placeholder - should be enhanced
    
    raise ValueError(f"Column definition extraction not implemented for {column_name}")

def _generate_view_alter_commands(obj: Dict) -> List[str]:
    """Generate ALTER commands for view changes."""
    commands = []
    
    # Get the original SQL statement
    original_sql = obj.get("view_definition", obj.get("query_text", ""))
    
    if obj["query_type"] == "view":
        # For views, replace CREATE with CREATE OR REPLACE
        or_replace_sql = _replace_create_view_with_or_replace(original_sql)
        commands.append(_ensure_semicolon(or_replace_sql))
    elif obj["query_type"] == "materialized_view":
        # Materialized views don't support CREATE OR REPLACE, so drop and recreate
        commands.append(f"DROP MATERIALIZED VIEW {obj['object_name']};")
        commands.append(_ensure_semicolon(original_sql))
    
    return commands

def _generate_function_alter_commands(obj: Dict) -> List[str]:
    """Generate ALTER commands for function changes."""
    commands = []
    
    # For functions, we typically drop and recreate
    commands.append(f"DROP FUNCTION {obj['object_name']};")
    commands.append(f"CREATE FUNCTION {obj['object_name']} {_extract_function_definition(obj)};")
    
    return commands

def _extract_table_definition(obj: Dict) -> str:
    """Extract table definition from pre-processed schema data."""
    # For now, return a placeholder - this should be enhanced to extract
    # column definitions from the query_text without using pglast directly
    return "/* Table definition placeholder */"

def _extract_view_definition(obj: Dict) -> str:
    """Extract view definition from pre-processed schema data."""
    # For now, return a placeholder - this should be enhanced to extract
    # the query part from the query_text without using pglast directly
    return "/* View definition placeholder */"

def _extract_function_definition(obj: Dict) -> str:
    """Extract function definition from pre-processed schema data."""
    # For now, return a placeholder - this should be enhanced to extract
    # function parameters and body from the query_text without using pglast directly
    return "/* Function definition placeholder */"

def _extract_index_definition(obj: Dict) -> str:
    """Extract index definition from pre-processed schema data."""
    # For now, return a placeholder - this should be enhanced to extract
    # index columns and options from the query_text without using pglast directly
    return "/* Index definition placeholder */"

def _ensure_semicolon(sql: str) -> str:
    """Ensure SQL statement ends with a semicolon for multi-query support."""
    sql = sql.strip()
    if not sql.endswith(';'):
        return sql + ';'
    return sql

def _replace_create_view_with_or_replace(sql: str) -> str:
    """Replace CREATE VIEW with CREATE OR REPLACE VIEW in SQL statement."""
    # Normalize whitespace and check for CREATE VIEW (case insensitive)
    normalized = sql.strip()
    if normalized.upper().startswith("CREATE VIEW"):
        # Find the position of "CREATE VIEW" (case insensitive)
        create_view_pos = normalized.upper().find("CREATE VIEW")
        if create_view_pos != -1:
            # Replace the original "CREATE VIEW" with "CREATE OR REPLACE VIEW"
            before_create = normalized[:create_view_pos]
            after_view = normalized[create_view_pos + len("CREATE VIEW"):]
            return before_create + "CREATE OR REPLACE VIEW" + after_view
    return sql 