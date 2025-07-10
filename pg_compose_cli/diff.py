from pglast import parse_sql
import json
from pg_compose_cli.ast_objects import ASTObject, ASTList, BuildStage

def ast_diff(a: dict, b: dict) -> dict:
    """Compare two schema objects using AST comparison."""
    if a["query_type"] != b["query_type"]:
        return {"type_changed": {"from": a["query_type"], "to": b["query_type"]}}
    
    if a["query_type"] == "base_table":
        return ast_diff_table(a, b)
    elif a["query_type"] in ["view", "materialized_view"]:
        return ast_diff_view(a, b)
    elif a["query_type"] == "function":
        return ast_diff_function(a, b)
    elif a["query_type"] == "grant":
        return ast_diff_grant(a, b)
    else:
        # For other types, compare the full AST
        return ast_diff_generic(a, b)

def ast_diff_table(a: dict, b: dict) -> dict:
    """Compare table definitions using AST."""
    try:
        a_stmt = parse_sql(a["query_text"])[0].stmt
        b_stmt = parse_sql(b["query_text"])[0].stmt
        
        # Compare columns
        a_cols = {col.colname: col for col in getattr(a_stmt, "tableElts", []) if hasattr(col, "colname")}
        b_cols = {col.colname: col for col in getattr(b_stmt, "tableElts", []) if hasattr(col, "colname")}
        
        added = []
        for col in b_cols.values():
            if col.colname not in a_cols:
                # Extract type
                if hasattr(col, "typeName") and hasattr(col.typeName, "names"):
                    names = [str(n.sval) for n in col.typeName.names]
                    if len(names) > 1 and names[0].lower() == "pg_catalog":
                        # Omit pg_catalog prefix
                        type_str = names[-1].upper()
                    else:
                        # Keep schema/type if not pg_catalog
                        type_str = ".".join(names).upper()
                else:
                    type_str = "TEXT"
                # Handle typmods (e.g., VARCHAR(15))
                if hasattr(col, "typeName") and hasattr(col.typeName, "typmods") and col.typeName.typmods:
                    mods = []
                    for mod in col.typeName.typmods:
                        if hasattr(mod, "val") and hasattr(mod.val, "ival"):
                            mods.append(str(mod.val.ival))
                        elif hasattr(mod, "ival"):
                            mods.append(str(mod.ival))
                        elif hasattr(mod, "val"):
                            mods.append(str(mod.val))
                        else:
                            mods.append(str(mod))
                    type_str += f"({', '.join(mods)})"
                
                col_def = f"{col.colname} {type_str}"
                if hasattr(col, "is_not_null") and col.is_not_null:
                    col_def += " NOT NULL"
                if hasattr(col, "raw_default") and col.raw_default:
                    default_val = col.raw_default.val
                    col_def += f" DEFAULT {default_val}"
                
                added.append(col_def)
        removed = [col.colname for col in a_cols.values() if col.colname not in b_cols]
        changed = []
        
        # Compare common columns
        for col_name in set(a_cols.keys()) & set(b_cols.keys()):
            a_col = a_cols[col_name]
            b_col = b_cols[col_name]
            
            # Compare type
            a_type = ".".join(str(n.sval) for n in a_col.typeName.names)
            b_type = ".".join(str(n.sval) for n in b_col.typeName.names)
            
            # Compare nullable
            a_nullable = not a_col.is_not_null
            b_nullable = not b_col.is_not_null
            
            # Compare default
            a_default = a_col.raw_default.val if a_col.raw_default else None
            b_default = b_col.raw_default.val if b_col.raw_default else None
            
            # Compare foreign key constraints
            a_fk = _extract_foreign_key_from_column(a_col)
            b_fk = _extract_foreign_key_from_column(b_col)
            
            if a_type != b_type or a_nullable != b_nullable or a_default != b_default or a_fk != b_fk:
                changed.append({
                    "column": col_name,
                    "type": {"from": a_type, "to": b_type} if a_type != b_type else None,
                    "nullable": {"from": a_nullable, "to": b_nullable} if a_nullable != b_nullable else None,
                    "default": {"from": a_default, "to": b_default} if a_default != b_default else None,
                    "foreign_key": {"from": a_fk, "to": b_fk} if a_fk != b_fk else None
                })
        
        # Compare primary keys
        a_pk = _extract_primary_key_from_table(a_stmt)
        b_pk = _extract_primary_key_from_table(b_stmt)
        primary_key_change = None
        
        if a_pk != b_pk:
            primary_key_change = {
                "from": a_pk,
                "to": b_pk
            }
        
        return {
            "add_columns": added,
            "drop_columns": removed,
            "change_columns": changed,
            "primary_key": primary_key_change
        }
        
    except Exception as e:
        error_msg = f"Failed to parse table AST: {str(e)}"
        if "GRANT" in a.get("query_text", "") or "GRANT" in b.get("query_text", ""):
            error_msg += "\nThis appears to be a GRANT statement parsing issue."
            error_msg += "\nConsider using the --no-grants flag to skip GRANT statements."
        return {"error": error_msg}

def ast_diff_view(a: dict, b: dict) -> dict:
    """Compare view definitions using AST."""
    try:
        a_stmt = parse_sql(a["query_text"])[0].stmt
        b_stmt = parse_sql(b["query_text"])[0].stmt
        
        # Compare the query part of views
        a_query = getattr(a_stmt, "query", None)
        b_query = getattr(b_stmt, "query", None)
        
        if a_query and b_query:
            # For now, return a simple indication that the view query changed
            # In a full implementation, you'd compare the query ASTs
            return {"query_changed": True}
        
        return {"no_changes": True}
        
    except Exception as e:
        error_msg = f"Failed to parse view AST: {str(e)}"
        if "GRANT" in a.get("query_text", "") or "GRANT" in b.get("query_text", ""):
            error_msg += "\nThis appears to be a GRANT statement parsing issue."
            error_msg += "\nConsider using the --no-grants flag to skip GRANT statements."
        return {"error": error_msg}

def ast_diff_function(a: dict, b: dict) -> dict:
    """Compare function definitions using AST."""
    try:
        a_stmt = parse_sql(a["query_text"])[0].stmt
        b_stmt = parse_sql(b["query_text"])[0].stmt
        
        # Compare function name
        a_name = ".".join(str(n.sval) for n in getattr(a_stmt, "funcname", []))
        b_name = ".".join(str(n.sval) for n in getattr(b_stmt, "funcname", []))
        
        if a_name != b_name:
            return {"name_changed": {"from": a_name, "to": b_name}}
        
        # Compare parameters
        a_params = getattr(a_stmt, "parameters", [])
        b_params = getattr(b_stmt, "parameters", [])
        
        if len(a_params) != len(b_params):
            return {"parameter_count_changed": {"from": len(a_params), "to": len(b_params)}}
        
        # For now, return a simple indication that the function changed
        return {"function_changed": True}
        
    except Exception as e:
        error_msg = f"Failed to parse function AST: {str(e)}"
        if "GRANT" in a.get("query_text", "") or "GRANT" in b.get("query_text", ""):
            error_msg += "\nThis appears to be a GRANT statement parsing issue."
            error_msg += "\nConsider using the --no-grants flag to skip GRANT statements."
        return {"error": error_msg}

def ast_diff_grant(a: dict, b: dict) -> dict:
    """Compare grant statements using AST."""
    try:
        a_stmt = parse_sql(a["query_text"])[0].stmt
        b_stmt = parse_sql(b["query_text"])[0].stmt
    except Exception as e:
        error_msg = f"Failed to parse grant AST: {str(e)}"
        if "GRANT" in a.get("query_text", "") or "GRANT" in b.get("query_text", ""):
            error_msg += "\nThis appears to be a GRANT statement parsing issue."
            error_msg += "\nConsider using the --no-grants flag to skip GRANT statements."
        return {"error": error_msg}
    
    # Compare privileges
    a_privileges = [priv.priv_name for priv in getattr(a_stmt, "privileges", [])]
    b_privileges = [priv.priv_name for priv in getattr(b_stmt, "privileges", [])]
    
    # Compare grantees
    a_grantees = [grant.rolename for grant in getattr(a_stmt, "grantees", [])]
    b_grantees = [grant.rolename for grant in getattr(b_stmt, "grantees", [])]
    
    # Compare objects
    a_objects = getattr(a_stmt, "objects", [])
    b_objects = getattr(b_stmt, "objects", [])
    
    changes = {}
    
    if a_privileges != b_privileges:
        changes["privileges"] = {"from": a_privileges, "to": b_privileges}
    
    if a_grantees != b_grantees:
        changes["grantees"] = {"from": a_grantees, "to": b_grantees}
    
    if len(changes) > 0:
        return changes
    else:
        return {"no_changes": True}

def _extract_foreign_key_from_column(col):
    """Extract foreign key information from a column."""
    if hasattr(col, 'constraints') and col.constraints:
        for constraint in col.constraints:
            if hasattr(constraint, 'contype') and constraint.contype == 5:  # CONSTR_FOREIGN
                # Extract referenced table and column
                if hasattr(constraint, 'pktable') and hasattr(constraint, 'pkattrs'):
                    ref_table = ".".join(str(n.sval) for n in constraint.pktable.names)
                    ref_col = constraint.pkattrs[0].sval if constraint.pkattrs else "id"
                    return f"{ref_table}({ref_col})"
    return None

def _extract_primary_key_from_table(table_stmt):
    """Extract primary key information from a table statement."""
    if hasattr(table_stmt, 'tableElts'):
        # Table-level constraints (composite PKs)
        for elem in table_stmt.tableElts:
            if hasattr(elem, 'contype') and elem.contype == 6:  # CONSTR_PRIMARY
                if hasattr(elem, 'keys'):
                    return [key.sval for key in elem.keys]
        # Column-level PKs
        for col in table_stmt.tableElts:
            if hasattr(col, 'colname') and getattr(col, 'constraints', None):
                for constraint in col.constraints or []:
                    if hasattr(constraint, 'contype') and constraint.contype == 6:  # CONSTR_PRIMARY
                        return col.colname
    return None

def extract_table_definition(obj: dict) -> str:
    """Extract table definition from CREATE TABLE statement."""
    try:
        stmt = parse_sql(obj["query_text"])[0].stmt
        
        if hasattr(stmt, "tableElts"):
            column_defs = []
            table_constraints = []
            for col in stmt.tableElts:
                if hasattr(col, "colname"):
                    # Extract type
                    if hasattr(col, "typeName") and hasattr(col.typeName, "names"):
                        names = [str(n.sval) for n in col.typeName.names]
                        if len(names) > 1 and names[0].lower() == "pg_catalog":
                            # Omit pg_catalog prefix
                            type_str = names[-1].upper()
                        else:
                            # Keep schema/type if not pg_catalog
                            type_str = ".".join(names).upper()
                    else:
                        type_str = "TEXT"
                    # Handle typmods (e.g., VARCHAR(15))
                    if hasattr(col, "typeName") and hasattr(col.typeName, "typmods") and col.typeName.typmods:
                        mods = []
                        for mod in col.typeName.typmods:
                            if hasattr(mod, "val") and hasattr(mod.val, "ival"):
                                mods.append(str(mod.val.ival))
                            elif hasattr(mod, "ival"):
                                mods.append(str(mod.ival))
                            elif hasattr(mod, "val"):
                                mods.append(str(mod.val))
                            else:
                                mods.append(str(mod))
                        type_str += f"({', '.join(mods)})"
                    col_def = f"{col.colname} {type_str}"
                    # Check for column-level constraints
                    if hasattr(col, 'constraints') and col.constraints:
                        for constraint in col.constraints:
                            if hasattr(constraint, 'contype') and constraint.contype == 6:  # CONSTR_PRIMARY
                                col_def += " PRIMARY KEY"
                            if hasattr(constraint, 'contype') and constraint.contype == 2:  # CONSTR_NOTNULL
                                col_def += " NOT NULL"
                            if hasattr(constraint, 'contype') and constraint.contype == 4:  # CONSTR_DEFAULT
                                if hasattr(constraint, 'raw_expr'):
                                    col_def += f" DEFAULT {constraint.raw_expr}"
                    if hasattr(col, "is_not_null") and col.is_not_null:
                        col_def += " NOT NULL"
                    if hasattr(col, "raw_default") and col.raw_default:
                        default_val = col.raw_default.val
                        col_def += f" DEFAULT {default_val}"
                    column_defs.append(f"  {col_def}")
                elif hasattr(col, 'contype') and col.contype == 6:  # Table-level PRIMARY KEY
                    if hasattr(col, 'keys'):
                        pk_cols = ', '.join(key.sval for key in col.keys)
                        table_constraints.append(f"  PRIMARY KEY ({pk_cols})")
            return ",\n".join(column_defs + table_constraints)
        return "/* No columns found */"
        
    except Exception as e:
        error_msg = f"Error extracting table definition: {str(e)}"
        if "GRANT" in obj.get("query_text", ""):
            error_msg += "\nThis appears to be a GRANT statement parsing issue."
            error_msg += "\nConsider using the --no-grants flag to skip GRANT statements."
        return f"/* {error_msg} */"

def extract_view_definition(obj: dict) -> str:
    """Extract view definition from CREATE VIEW statement."""
    try:
        # Just return the original SQL - the alter generator will handle CREATE OR REPLACE
        return obj["query_text"]
    except Exception as e:
        return f"/* Error extracting view definition: {str(e)} */"

def ast_diff_generic(a: dict, b: dict) -> dict:
    """Compare generic schema objects using AST."""
    try:
        # For generic objects, compare the full AST structure
        a_stmt = parse_sql(a["query_text"])[0].stmt
        b_stmt = parse_sql(b["query_text"])[0].stmt
        
        # Convert ASTs to comparable format (simplified)
        a_ast_str = str(type(a_stmt).__name__)
        b_ast_str = str(type(b_stmt).__name__)
        
        if a_ast_str != b_ast_str:
            return {"ast_type_changed": {"from": a_ast_str, "to": b_ast_str}}
        
        return {"ast_changed": True}
        
    except Exception as e:
        error_msg = f"Failed to parse generic AST: {str(e)}"
        if "GRANT" in a.get("query_text", "") or "GRANT" in b.get("query_text", ""):
            error_msg += "\nThis appears to be a GRANT statement parsing issue."
            error_msg += "\nConsider using the --no-grants flag to skip GRANT statements."
        return {"error": error_msg}

def diff_schemas(base: list[dict], updated: list[dict]) -> ASTList:
    base_map = {f"{q['query_type']}:{q['object_name']}": q for q in base}
    updated_map = {f"{q['query_type']}:{q['object_name']}": q for q in updated}

    ast_objects = []

    all_keys = set(base_map) | set(updated_map)

    for key in sorted(all_keys):
        if key not in base_map:
            # Created object
            obj = updated_map[key]
            # Add definitions for created objects
            if obj["query_type"] == "base_table":
                obj["table_definition"] = extract_table_definition(obj)
            elif obj["query_type"] in ["view", "materialized_view"]:
                obj["view_definition"] = extract_view_definition(obj)
            
            # Create ASTObject for created object
            ast_objects.append(ASTObject(
                command=obj["query_text"],
                object_name=obj["object_name"],
                query_type=BuildStage(obj["query_type"]),
                dependencies=obj.get("dependencies", []),
                query_hash=obj["query_hash"],
                schema=obj.get("schema")
            ))
        elif key not in updated_map:
            # Dropped object
            obj = base_map[key]
            # Create DROP command
            if obj["query_type"] == "base_table":
                command = f"DROP TABLE {obj['object_name']};"
            elif obj["query_type"] == "view":
                command = f"DROP VIEW {obj['object_name']};"
            elif obj["query_type"] == "materialized_view":
                command = f"DROP MATERIALIZED VIEW {obj['object_name']};"
            elif obj["query_type"] == "function":
                command = f"DROP FUNCTION {obj['object_name']};"
            elif obj["query_type"] == "index":
                command = f"DROP INDEX {obj['object_name']};"
            elif obj["query_type"] == "grant":
                # For grants, we'll handle this in the deploy logic
                continue
            else:
                command = f"DROP {obj['query_type'].upper()} {obj['object_name']};"
            
            ast_objects.append(ASTObject(
                command=command,
                object_name=obj["object_name"],
                query_type=BuildStage(obj["query_type"]),
                dependencies=obj.get("dependencies", []),
                query_hash=obj["query_hash"],
                schema=obj.get("schema")
            ))
        elif base_map[key]["query_hash"] != updated_map[key]["query_hash"]:
            # Changed object
            a = base_map[key]
            b = updated_map[key]
            ast_diff_result = ast_diff(a, b)
            
            # Add definitions for changed objects
            if a["query_type"] == "base_table":
                table_definition = extract_table_definition(b)
            elif a["query_type"] in ["view", "materialized_view"]:
                view_definition = extract_view_definition(b)
            
            # Create ALTER commands based on the diff
            alter_commands = _generate_alter_commands_from_diff(a, b, ast_diff_result)
            for i, cmd in enumerate(alter_commands):
                # Generate unique hash for each alter command
                import hashlib
                unique_hash = hashlib.sha256(f"{a['query_hash']}_{i}_{cmd}".encode()).hexdigest()
                ast_objects.append(ASTObject(
                    command=cmd,
                    object_name=a["object_name"],
                    query_type=BuildStage(a["query_type"]),
                    dependencies=a.get("dependencies", []),
                    query_hash=unique_hash,
                    schema=a.get("schema")
                ))

    return ASTList(ast_objects)


def _generate_alter_commands_from_diff(a: dict, b: dict, ast_diff_result: dict) -> list[str]:
    """Generate ALTER commands from a diff result."""
    commands = []
    
    if a["query_type"] == "base_table":
        # Handle table changes
        if "add_columns" in ast_diff_result:
            for col_def in ast_diff_result["add_columns"]:
                commands.append(f"ALTER TABLE {a['object_name']} ADD COLUMN {col_def};")
        
        if "drop_columns" in ast_diff_result:
            for col_name in ast_diff_result["drop_columns"]:
                commands.append(f"ALTER TABLE {a['object_name']} DROP COLUMN {col_name};")
        
        if "change_columns" in ast_diff_result:
            for change in ast_diff_result["change_columns"]:
                column_name = change["column"]
                
                if change.get("type"):
                    new_type = change["type"]["to"]
                    commands.append(f"ALTER TABLE {a['object_name']} ALTER COLUMN {column_name} TYPE {new_type};")
                
                if change.get("nullable") is not None:
                    is_nullable = change["nullable"]["to"]
                    constraint = "DROP NOT NULL" if is_nullable else "SET NOT NULL"
                    commands.append(f"ALTER TABLE {a['object_name']} ALTER COLUMN {column_name} {constraint};")
                
                if change.get("default") is not None:
                    new_default = change["default"]["to"]
                    if new_default is None:
                        commands.append(f"ALTER TABLE {a['object_name']} ALTER COLUMN {column_name} DROP DEFAULT;")
                    else:
                        commands.append(f"ALTER TABLE {a['object_name']} ALTER COLUMN {column_name} SET DEFAULT {new_default};")
                
                if change.get("foreign_key") is not None:
                    fk_change = change["foreign_key"]
                    old_fk = fk_change.get("from")
                    new_fk = fk_change.get("to")
                    
                    if old_fk:
                        # Drop existing foreign key constraint
                        # We need to find the constraint name - for now, use a generic approach
                        commands.append(f"ALTER TABLE {a['object_name']} DROP CONSTRAINT IF EXISTS {a['object_name']}_{column_name}_fkey;")
                    
                    if new_fk:
                        # Add new foreign key constraint
                        ref_table, ref_col = new_fk.split("(")
                        ref_col = ref_col.rstrip(")")
                        commands.append(f"ALTER TABLE {a['object_name']} ADD CONSTRAINT {a['object_name']}_{column_name}_fkey FOREIGN KEY ({column_name}) REFERENCES {ref_table}({ref_col});")
        
        if "primary_key" in ast_diff_result and ast_diff_result["primary_key"] is not None:
            pk_change = ast_diff_result["primary_key"]
            old_pk = pk_change.get("from")
            new_pk = pk_change.get("to")
            
            if old_pk:
                commands.append(f"ALTER TABLE {a['object_name']} DROP CONSTRAINT {old_pk};")
            
            if new_pk:
                if isinstance(new_pk, list):
                    pk_columns = ", ".join(new_pk)
                    commands.append(f"ALTER TABLE {a['object_name']} ADD PRIMARY KEY ({pk_columns});")
                else:
                    commands.append(f"ALTER TABLE {a['object_name']} ADD PRIMARY KEY ({new_pk});")
    
    elif a["query_type"] in ["view", "materialized_view"]:
        # Handle view changes
        if a["query_type"] == "view":
            # For views, use CREATE OR REPLACE
            original_sql = b.get("view_definition", b["query_text"])
            or_replace_sql = original_sql.replace("CREATE VIEW", "CREATE OR REPLACE VIEW")
            commands.append(or_replace_sql)
        elif a["query_type"] == "materialized_view":
            # Materialized views don't support CREATE OR REPLACE, so drop and recreate
            commands.append(f"DROP MATERIALIZED VIEW {a['object_name']};")
            commands.append(b.get("view_definition", b["query_text"]))
    
    elif a["query_type"] == "function":
        # Handle function changes
        commands.append(f"DROP FUNCTION {a['object_name']};")
        commands.append(f"CREATE FUNCTION {a['object_name']} {b['query_text']};")
    
    elif a["query_type"] == "grant":
        # Handle grant changes
        # For now, just revoke and grant
        commands.append(f"REVOKE ALL ON {a['object_name']} FROM PUBLIC;")
        commands.append(b["query_text"])
    
    return commands