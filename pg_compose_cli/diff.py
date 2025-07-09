from pglast import parse_sql
import json

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
            
            if a_type != b_type or a_nullable != b_nullable or a_default != b_default:
                changed.append({
                    "column": col_name,
                    "type": {"from": a_type, "to": b_type} if a_type != b_type else None,
                    "nullable": {"from": a_nullable, "to": b_nullable} if a_nullable != b_nullable else None,
                    "default": {"from": a_default, "to": b_default} if a_default != b_default else None
                })
        
        return {
            "add_columns": added,
            "drop_columns": removed,
            "change_columns": changed
        }
    except Exception as e:
        return {"error": f"Failed to parse table AST: {str(e)}"}

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
        return {"error": f"Failed to parse view AST: {str(e)}"}

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
        return {"error": f"Failed to parse function AST: {str(e)}"}

def extract_table_definition(obj: dict) -> str:
    """Extract table definition from CREATE TABLE statement."""
    try:
        stmt = parse_sql(obj["query_text"])[0].stmt
        if hasattr(stmt, "tableElts"):
            column_defs = []
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
                    if hasattr(col, "is_not_null") and col.is_not_null:
                        col_def += " NOT NULL"
                    if hasattr(col, "raw_default") and col.raw_default:
                        default_val = col.raw_default.val
                        col_def += f" DEFAULT {default_val}"
                    
                    column_defs.append(f"  {col_def}")
            
            return ",\n".join(column_defs)
        
        return "/* No columns found */"
    except Exception as e:
        return f"/* Error extracting table definition: {str(e)} */"

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
        return {"error": f"Failed to parse generic AST: {str(e)}"}

def diff_schemas(base: list[dict], updated: list[dict]) -> dict:
    base_map = {f"{q['query_type']}:{q['object_name']}": q for q in base}
    updated_map = {f"{q['query_type']}:{q['object_name']}": q for q in updated}

    created = []
    dropped = []
    changed = []

    all_keys = set(base_map) | set(updated_map)

    for key in sorted(all_keys):
        if key not in base_map:
            obj = updated_map[key]
            # Add definitions for created objects
            if obj["query_type"] == "base_table":
                obj["table_definition"] = extract_table_definition(obj)
            elif obj["query_type"] in ["view", "materialized_view"]:
                obj["view_definition"] = extract_view_definition(obj)
            created.append(obj)
        elif key not in updated_map:
            dropped.append(base_map[key])
        elif base_map[key]["query_hash"] != updated_map[key]["query_hash"]:
            a = base_map[key]
            b = updated_map[key]
            ast_diff_result = ast_diff(a, b)
            changed_obj = {
                "object_name": a["object_name"],
                "query_type": a["query_type"],
                "from_hash": a["query_hash"],
                "to_hash": b["query_hash"],
                "ast_diff": ast_diff_result
            }
            # Add definitions for changed objects
            if a["query_type"] == "base_table":
                changed_obj["table_definition"] = extract_table_definition(b)
            elif a["query_type"] in ["view", "materialized_view"]:
                changed_obj["view_definition"] = extract_view_definition(b)
            changed.append(changed_obj)

    return {"created": created, "dropped": dropped, "changed": changed}