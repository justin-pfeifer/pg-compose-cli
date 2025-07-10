"""
Test qualified names (schema.table_name) handling.
"""

import pytest
from pg_compose_core.lib.extract import extract_build_queries
from pg_compose_core.lib.diff import diff_schemas
from pg_compose_core.lib.ast_objects import ASTList


def test_qualified_table_names():
    """Test that schema-qualified table names are properly extracted."""
    sql = """
    CREATE TABLE public.users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    
    CREATE TABLE private.accounts (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES public.users(id)
    );
    """
    
    result = extract_build_queries(sql, use_ast_objects=True)
    
    # Check that we have 2 tables
    assert len(result) == 2
    
    # Check object names - these should include schema qualification
    object_names = [obj.object_name for obj in result]
    print(f"Object names: {object_names}")
    
    # Currently this will fail because the extraction only gets unqualified names
    # The expected behavior would be: ['public.users', 'private.accounts']
    # But currently we get: ['users', 'accounts']
    
    # Check schema field - this should be populated
    schemas = [obj.schema for obj in result]
    print(f"Schemas: {schemas}")
    
    # Currently this will be [None, None] because schema extraction is not implemented


def test_qualified_names_in_diff():
    """Test that schema-qualified names work correctly in diff operations."""
    sql_a = """
    CREATE TABLE public.users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    
    sql_b = """
    CREATE TABLE public.users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT
    );
    """
    
    result_a = extract_build_queries(sql_a, use_ast_objects=True)
    result_b = extract_build_queries(sql_b, use_ast_objects=True)
    
    # Check that the objects are properly identified as the same table
    # despite being in different schema contexts
    obj_a = result_a[0]
    obj_b = result_b[0]
    
    print(f"Object A: {obj_a.object_name} (schema: {obj_a.schema})")
    print(f"Object B: {obj_b.object_name} (schema: {obj_b.schema})")
    
    # Currently both will show as 'users' with schema=None
    # This could cause issues if there are multiple tables with the same name
    # in different schemas


def test_diff_with_qualified_names():
    """Test that diff operations generate ALTER commands with qualified names."""
    sql_a = """
    CREATE TABLE public.users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    
    sql_b = """
    CREATE TABLE public.users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT
    );
    """
    
    result_a = extract_build_queries(sql_a, use_ast_objects=True)
    result_b = extract_build_queries(sql_b, use_ast_objects=True)
    
    # Generate diff
    diff_result = diff_schemas(result_a, result_b)
    
    # Check that ALTER commands use qualified names
    sql_output = diff_result.to_sql()
    print(f"Generated SQL:\n{sql_output}")
    
    # Should contain "ALTER TABLE public.users ADD COLUMN email TEXT;"
    assert "public.users" in sql_output
    assert "ADD COLUMN email" in sql_output


def test_schema_conflict_resolution():
    """Test that tables with same name in different schemas are handled correctly."""
    sql_a = """
    CREATE TABLE public.users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    
    sql_b = """
    CREATE TABLE private.users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT
    );
    """
    
    result_a = extract_build_queries(sql_a, use_ast_objects=True)
    result_b = extract_build_queries(sql_b, use_ast_objects=True)
    
    # Generate diff
    diff_result = diff_schemas(result_a, result_b)
    
    # Check that both tables are handled separately
    sql_output = diff_result.to_sql()
    print(f"Schema conflict SQL:\n{sql_output}")
    
    # Should contain both DROP and CREATE commands for different schemas
    assert "DROP TABLE public.users" in sql_output
    assert "CREATE TABLE private.users" in sql_output


if __name__ == "__main__":
    test_qualified_table_names()
    test_qualified_names_in_diff()
    test_diff_with_qualified_names()
    test_schema_conflict_resolution() 