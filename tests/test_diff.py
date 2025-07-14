"""
Tests for the simplified diff module.
"""

import pytest
from pg_compose_core.lib.diff import diff_schemas
from pg_compose_core.lib.parser import parse_sql_to_ast_objects
from pg_compose_core.lib.ast import BuildStage


def test_diff_new_table():
    """Test diff when a new table is added."""
    base_sql = ""
    updated_sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    
    base = parse_sql_to_ast_objects(base_sql)
    updated = parse_sql_to_ast_objects(updated_sql)
    
    result = diff_schemas(base, updated)
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.BASE_TABLE
    assert obj.object_name == "users"
    assert "CREATE TABLE users" in obj.command


def test_diff_dropped_table():
    """Test diff when a table is dropped."""
    base_sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    updated_sql = ""
    
    base = parse_sql_to_ast_objects(base_sql)
    updated = parse_sql_to_ast_objects(updated_sql)
    
    result = diff_schemas(base, updated)
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.UNKNOWN
    assert "DROP TABLE users" in obj.command


def test_diff_new_index():
    """Test diff when a new index is added."""
    base_sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    updated_sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    
    CREATE INDEX idx_users_name ON users(name);
    """
    
    base = parse_sql_to_ast_objects(base_sql)
    updated = parse_sql_to_ast_objects(updated_sql)
    
    result = diff_schemas(base, updated)
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.INDEX
    assert obj.object_name == "idx_users_name"
    assert "CREATE INDEX idx_users_name" in obj.command


def test_diff_dropped_index():
    """Test diff when an index is dropped."""
    base_sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    
    CREATE INDEX idx_users_name ON users(name);
    """
    updated_sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    
    base = parse_sql_to_ast_objects(base_sql)
    updated = parse_sql_to_ast_objects(updated_sql)
    
    result = diff_schemas(base, updated)
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.UNKNOWN
    assert "DROP INDEX idx_users_name" in obj.command


def test_diff_qualified_names():
    """Test diff with schema-qualified names."""
    base_sql = """
    CREATE TABLE public.users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    updated_sql = """
    CREATE TABLE public.users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT
    );
    
    CREATE INDEX idx_users_email ON public.users(email);
    """
    
    base = parse_sql_to_ast_objects(base_sql)
    updated = parse_sql_to_ast_objects(updated_sql)
    
    result = diff_schemas(base, updated)
    
    # Should have the new index
    index_obj = next((obj for obj in result if obj.query_type == BuildStage.INDEX), None)
    assert index_obj is not None
    assert index_obj.object_name == "idx_users_email"
    assert "public.users" in index_obj.dependencies


def test_diff_grant_changes():
    """Test diff when grants change."""
    base_sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    
    GRANT SELECT ON users TO app_user;
    """
    updated_sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    
    GRANT SELECT, INSERT ON users TO app_user;
    """
    
    base = parse_sql_to_ast_objects(base_sql)
    updated = parse_sql_to_ast_objects(updated_sql)
    
    result = diff_schemas(base, updated)
    
    # Should have revoke and new grant
    assert len(result) == 2
    revoke_obj = next((obj for obj in result if "REVOKE" in obj.command), None)
    grant_obj = next((obj for obj in result if "GRANT" in obj.command and "REVOKE" not in obj.command), None)
    
    assert revoke_obj is not None
    assert grant_obj is not None
    assert "GRANT SELECT, INSERT ON users TO app_user" in grant_obj.command


def test_diff_no_changes():
    """Test diff when there are no changes."""
    sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    
    base = parse_sql_to_ast_objects(sql)
    updated = parse_sql_to_ast_objects(sql)
    
    result = diff_schemas(base, updated)
    
    assert len(result) == 0


def test_diff_multiple_changes():
    """Test diff with multiple types of changes."""
    base_sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    
    CREATE INDEX idx_users_name ON users(name);
    
    GRANT SELECT ON users TO app_user;
    """
    updated_sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT
    );
    
    CREATE INDEX idx_users_name ON users(name);
    CREATE INDEX idx_users_email ON users(email);
    
    GRANT SELECT, INSERT ON users TO app_user;
    """
    
    base = parse_sql_to_ast_objects(base_sql)
    updated = parse_sql_to_ast_objects(updated_sql)
    
    result = diff_schemas(base, updated)
    
    # Should have new index and grant changes
    assert len(result) >= 3  # New index + revoke + new grant
    
    # Check for new index
    index_obj = next((obj for obj in result if obj.object_name == "idx_users_email"), None)
    assert index_obj is not None
    assert index_obj.query_type == BuildStage.INDEX 