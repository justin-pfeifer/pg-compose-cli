"""
Simplified diff logic that works with ASTObjects from parser.py.
Generates migration commands by comparing two ASTLists.
"""

from typing import List, Optional
from pg_compose_core.lib.ast.objects import ASTObject, BuildStage
from pg_compose_core.lib.ast.function import FunctionASTObject
from pg_compose_core.lib.ast.list import ASTList

def diff_schemas(base: ASTList, updated: ASTList) -> ASTList:
    """
    Generate migration commands by comparing two ASTLists.
    Returns an ASTList containing CREATE/DROP/ALTER commands.
    """
    # Create keys that include schema information to avoid conflicts
    def make_key(obj: ASTObject) -> str:
        return f"{obj.query_type.value}:{obj.qualified_name}"
    
    base_map = {make_key(obj): obj for obj in base}
    updated_map = {make_key(obj): obj for obj in updated}
    
    migration_commands = []
    
    # Find all unique keys
    all_keys = set(base_map.keys()) | set(updated_map.keys())
    
    for key in sorted(all_keys):
        if key not in base_map:
            # New object - CREATE it
            obj = updated_map[key]
            migration_commands.append(obj)
            
        elif key not in updated_map:
            # Dropped object - DROP it
            obj = base_map[key]
            drop_command = _generate_drop_command(obj)
            if drop_command:
                migration_commands.append(drop_command)
                
        elif base_map[key].query_hash != updated_map[key].query_hash:
            # Changed object - generate ALTER commands
            old_obj = base_map[key]
            new_obj = updated_map[key]
            alter_commands = _generate_alter_commands(old_obj, new_obj)
            migration_commands.extend(alter_commands)
    
    return ASTList(migration_commands)

def _generate_drop_command(obj: ASTObject) -> ASTObject:
    """Generate a DROP command for an object."""
    # Use qualified name
    object_name = obj.qualified_name
    
    if obj.query_type == BuildStage.BASE_TABLE:
        command = f"DROP TABLE {object_name};"
    elif obj.query_type == BuildStage.VIEW:
        command = f"DROP VIEW {object_name};"
    elif obj.query_type == BuildStage.MATERIALIZED_VIEW:
        command = f"DROP MATERIALIZED VIEW {object_name};"
    elif obj.query_type == BuildStage.FUNCTION:
        if isinstance(obj, FunctionASTObject) and obj.parameters:
            # Include parameter types in DROP for function overloading
            param_strs = [f"{param.data_type}" for param in obj.parameters]
            command = f"DROP FUNCTION {object_name}({', '.join(param_strs)});"
        else:
            command = f"DROP FUNCTION {object_name};"
    elif obj.query_type == BuildStage.PROCEDURE:
        if isinstance(obj, FunctionASTObject) and obj.parameters:
            # Include parameter types in DROP for procedure overloading
            param_strs = [f"{param.data_type}" for param in obj.parameters]
            command = f"DROP PROCEDURE {object_name}({', '.join(param_strs)});"
        else:
            command = f"DROP PROCEDURE {object_name};"
    elif obj.query_type == BuildStage.INDEX:
        command = f"DROP INDEX {object_name};"
    elif obj.query_type == BuildStage.CONSTRAINT:
        command = f"ALTER TABLE {object_name.split('.')[-1]} DROP CONSTRAINT {obj.object_name};"
    elif obj.query_type == BuildStage.POLICY:
        table_name = object_name.split('.')[-1] if '.' in object_name else obj.object_name
        command = f"DROP POLICY {obj.object_name} ON {table_name};"
    elif obj.query_type == BuildStage.GRANT:
        # For grants, generate a REVOKE command by parsing the SQL
        from pg_compose_core.lib.parser import parse_sql_to_ast_objects
        
        # Extract original object name from grant object name
        # Format: grant_PRIVILEGES_on_OBJECT_to_GRANTEE
        grant_parts = obj.object_name.split('_')
        if len(grant_parts) >= 5 and grant_parts[0] == 'grant':
            # Find the 'on' and 'to' parts to extract object name
            try:
                on_index = grant_parts.index('on')
                to_index = grant_parts.index('to')
                if on_index < to_index:
                    object_name = '_'.join(grant_parts[on_index + 1:to_index])
                    privileges = grant_parts[1:on_index]
                    grantees = grant_parts[to_index + 1:]
                    
                    # Generate revoke command
                    privilege_str = ' '.join(privileges) if privileges else 'ALL'
                    grantee_str = ' '.join(grantees) if grantees else 'PUBLIC'
                    revoke_sql = f"REVOKE {privilege_str} ON {object_name} FROM {grantee_str};"
                    revoke_objects = parse_sql_to_ast_objects(revoke_sql, grants=True)
                    if revoke_objects:
                        return revoke_objects[0]
            except ValueError:
                # Fallback if parsing fails
                pass
        
        # Fallback: generate a generic revoke command
        revoke_sql = f"REVOKE ALL ON {obj.object_name.replace('grant_', '').split('_on_')[1].split('_to_')[0]} FROM PUBLIC;"
        revoke_objects = parse_sql_to_ast_objects(revoke_sql, grants=True)
        if revoke_objects:
            return revoke_objects[0]
        return None
    else:
        command = f"DROP {obj.query_type.value.upper()} {object_name};"
    
    return ASTObject(
        command=command,
        object_name=obj.object_name,
        query_type=BuildStage.UNKNOWN,
        dependencies=obj.dependencies,
        schema=obj.schema
    )

def _generate_alter_commands(old_obj: ASTObject, new_obj: ASTObject) -> List[ASTObject]:
    """Generate ALTER commands for changed objects."""
    commands = []
    
    if old_obj.query_type == BuildStage.BASE_TABLE:
        # For tables, we need to parse the AST to find column changes
        table_commands = _generate_table_alter_commands(old_obj, new_obj)
        commands.extend(table_commands)
        
    elif old_obj.query_type == BuildStage.VIEW:
        # For views, drop and recreate
        drop_cmd = _generate_drop_command(old_obj)
        if drop_cmd:
            commands.append(drop_cmd)
        commands.append(new_obj)
        
    elif old_obj.query_type == BuildStage.FUNCTION:
        # For functions, check if signature changed
        if isinstance(old_obj, FunctionASTObject) and isinstance(new_obj, FunctionASTObject):
            if old_obj.signature_matches(new_obj):
                # Same signature, body-only change - use CREATE OR REPLACE
                commands.append(new_obj)
            else:
                # Signature changed - need DROP + CREATE
                drop_cmd = _generate_drop_command(old_obj)
                if drop_cmd:
                    commands.append(drop_cmd)
                commands.append(new_obj)
        else:
            # Fallback for non-FunctionASTObject functions
            drop_cmd = _generate_drop_command(old_obj)
            if drop_cmd:
                commands.append(drop_cmd)
            commands.append(new_obj)
    
    elif old_obj.query_type == BuildStage.PROCEDURE:
        # For procedures, check if signature changed
        if isinstance(old_obj, FunctionASTObject) and isinstance(new_obj, FunctionASTObject):
            if old_obj.signature_matches(new_obj):
                # Same signature, body-only change - use CREATE OR REPLACE
                commands.append(new_obj)
            else:
                # Signature changed - need DROP + CREATE
                drop_cmd = _generate_drop_command(old_obj)
                if drop_cmd:
                    commands.append(drop_cmd)
                commands.append(new_obj)
        else:
            # Fallback for non-FunctionASTObject procedures
            drop_cmd = _generate_drop_command(old_obj)
            if drop_cmd:
                commands.append(drop_cmd)
            commands.append(new_obj)
        
    elif old_obj.query_type == BuildStage.GRANT:
        # For grants, revoke old and grant new
        # Parse the GRANT statements to get proper AST objects with unique query hashes
        from pg_compose_core.lib.parser import parse_sql_to_ast_objects
        
        # Extract original object name, privileges, and grantees from old grant object name
        # Format: grant_PRIVILEGES_on_OBJECT_to_GRANTEE
        old_grant_parts = old_obj.object_name.split('_')
        if len(old_grant_parts) >= 5 and old_grant_parts[0] == 'grant':
            try:
                on_index = old_grant_parts.index('on')
                to_index = old_grant_parts.index('to')
                if on_index < to_index:
                    object_name = '_'.join(old_grant_parts[on_index + 1:to_index])
                    privileges = old_grant_parts[1:on_index]
                    grantees = old_grant_parts[to_index + 1:]
                    
                    # Generate revoke command
                    privilege_str = ' '.join(privileges) if privileges else 'ALL'
                    grantee_str = ' '.join(grantees) if grantees else 'PUBLIC'
                    revoke_sql = f"REVOKE {privilege_str} ON {object_name} FROM {grantee_str};"
                    revoke_objects = parse_sql_to_ast_objects(revoke_sql, grants=True)
                    if revoke_objects:
                        commands.extend(revoke_objects)
            except ValueError:
                # Fallback if parsing fails
                revoke_sql = f"REVOKE ALL ON {old_obj.object_name.replace('grant_', '').split('_on_')[1].split('_to_')[0]} FROM PUBLIC;"
                revoke_objects = parse_sql_to_ast_objects(revoke_sql, grants=True)
                if revoke_objects:
                    commands.extend(revoke_objects)
        
        # Add the new grant object (which should already be properly parsed)
        commands.append(new_obj)
    
    return commands

def _generate_table_alter_commands(old_obj: ASTObject, new_obj: ASTObject) -> List[ASTObject]:
    """Generate ALTER TABLE commands for table changes."""
    commands = []
    
    # Use qualified name
    table_name = new_obj.qualified_name
    
    # Check if both objects are TableASTObject instances
    from pg_compose_core.lib.ast.table import TableASTObject
    
    if isinstance(old_obj, TableASTObject) and isinstance(new_obj, TableASTObject):
        # Get the differences
        added_columns, removed_columns, changed_columns = new_obj.diff(old_obj)
        
        # Generate ALTER statements for added columns
        for column in added_columns:
            alter_command = f"ALTER TABLE {table_name} ADD COLUMN {column.name} {column.data_type}"
            if not column.is_nullable:
                alter_command += " NOT NULL"
            if column.default:
                alter_command += f" DEFAULT {column.default}"
            alter_command += ";"
            
            commands.append(ASTObject(
                command=alter_command,
                object_name=new_obj.object_name,
                query_type=BuildStage.UNKNOWN,
                dependencies=new_obj.dependencies,
                schema=new_obj.schema
            ))
        
        # Generate ALTER statements for removed columns
        for column in removed_columns:
            alter_command = f"ALTER TABLE {table_name} DROP COLUMN {column.name};"
            
            commands.append(ASTObject(
                command=alter_command,
                object_name=new_obj.object_name,
                query_type=BuildStage.UNKNOWN,
                dependencies=new_obj.dependencies,
                schema=new_obj.schema
            ))
        
        # Generate ALTER statements for changed columns
        for old_col, new_col in changed_columns:
            # Type changes
            if old_col.data_type != new_col.data_type:
                alter_command = f"ALTER TABLE {table_name} ALTER COLUMN {new_col.name} TYPE {new_col.data_type};"
                
                commands.append(ASTObject(
                    command=alter_command,
                    object_name=new_obj.object_name,
                    query_type=BuildStage.UNKNOWN,
                    dependencies=new_obj.dependencies,
                    schema=new_obj.schema
                ))
            
            # Nullability changes
            if old_col.is_nullable != new_col.is_nullable:
                if new_col.is_nullable:
                    alter_command = f"ALTER TABLE {table_name} ALTER COLUMN {new_col.name} DROP NOT NULL;"
                else:
                    alter_command = f"ALTER TABLE {table_name} ALTER COLUMN {new_col.name} SET NOT NULL;"
                
                commands.append(ASTObject(
                    command=alter_command,
                    object_name=new_obj.object_name,
                    query_type=BuildStage.UNKNOWN,
                    dependencies=new_obj.dependencies,
                    schema=new_obj.schema
                ))
            
            # Default changes
            if old_col.default != new_col.default:
                if new_col.default is None:
                    alter_command = f"ALTER TABLE {table_name} ALTER COLUMN {new_col.name} DROP DEFAULT;"
                else:
                    alter_command = f"ALTER TABLE {table_name} ALTER COLUMN {new_col.name} SET DEFAULT {new_col.default};"
                
                commands.append(ASTObject(
                    command=alter_command,
                    object_name=new_obj.object_name,
                    query_type=BuildStage.UNKNOWN,
                    dependencies=new_obj.dependencies,
                    schema=new_obj.schema
                ))
        
        # TODO: Add constraint comparison logic here
        # This would compare old_obj.constraints vs new_obj.constraints
        # and generate ADD/DROP CONSTRAINT statements
        
    else:
        # Fallback for non-TableASTObject instances
        if old_obj.query_hash != new_obj.query_hash:
            alter_command = f"ALTER TABLE {table_name} ADD COLUMN new_column TEXT; -- TODO: Implement proper column comparison"
            
            commands.append(ASTObject(
                command=alter_command,
                object_name=new_obj.object_name,
                query_type=BuildStage.UNKNOWN,
                dependencies=new_obj.dependencies,
                schema=new_obj.schema
            ))
    
    return commands

# Legacy compatibility function
def compare_sources(source_a: str, source_b: str, schemas: Optional[List[str]] = None, grants: bool = True) -> ASTList:
    """Legacy function for backward compatibility."""
    from pg_compose_core.lib.parser import load_source
    
    # Load both sources
    schema_a = load_source(source_a, schemas=schemas, grants=grants)
    schema_b = load_source(source_b, schemas=schemas, grants=grants)
    
    # Generate diff
    return diff_schemas(schema_a, schema_b)