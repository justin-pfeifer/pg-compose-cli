"""
Tests for the simplified parser module.
"""

import pytest
from pg_compose_core.lib.parser import parse_sql_to_ast_objects, extract_build_queries
from pg_compose_core.lib.ast import BuildStage


def test_parse_simple_table():
    """Test parsing a simple CREATE TABLE statement."""
    sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    
    result = parse_sql_to_ast_objects(sql)
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.BASE_TABLE
    assert obj.object_name == "users"
    assert obj.schema is None
    assert "CREATE TABLE users" in obj.command


def test_parse_qualified_table():
    """Test parsing a schema-qualified CREATE TABLE statement."""
    sql = """
    CREATE TABLE public.users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    
    result = parse_sql_to_ast_objects(sql)
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.BASE_TABLE
    assert obj.object_name == "users"
    assert obj.schema == "public"
    assert obj.qualified_name == "public.users"
    assert "CREATE TABLE public.users" in obj.command


def test_parse_index():
    """Test parsing a CREATE INDEX statement."""
    sql = """
    CREATE INDEX idx_users_name ON users(name);
    """
    
    result = parse_sql_to_ast_objects(sql)
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.INDEX
    assert obj.object_name == "idx_users_name"
    assert obj.schema is None
    assert "users" in obj.dependencies
    assert "CREATE INDEX idx_users_name" in obj.command


def test_parse_qualified_index():
    """Test parsing a schema-qualified CREATE INDEX statement."""
    sql = """
    CREATE INDEX idx_users_name ON public.users(name);
    """
    
    result = parse_sql_to_ast_objects(sql)
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.INDEX
    assert obj.object_name == "idx_users_name"
    assert obj.schema == "public"
    assert obj.qualified_name == "public.idx_users_name"
    assert "public.users" in obj.dependencies
    assert "CREATE INDEX idx_users_name" in obj.command


def test_parse_grant():
    """Test parsing a GRANT statement."""
    sql = """
    GRANT SELECT ON users TO app_user;
    """
    
    result = parse_sql_to_ast_objects(sql)
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.GRANT
    assert obj.object_name == "users"
    assert obj.resource_type.value == "table"
    assert "users" in obj.dependencies
    assert "GRANT SELECT ON users" in obj.command


def test_parse_qualified_grant():
    """Test parsing a schema-qualified GRANT statement."""
    sql = """
    GRANT SELECT ON public.users TO app_user;
    """
    
    result = parse_sql_to_ast_objects(sql)
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.GRANT
    assert obj.object_name == "users"
    assert obj.schema == "public"
    assert obj.qualified_name == "public.users"
    assert obj.resource_type.value == "table"
    assert "public.users" in obj.dependencies
    assert "GRANT SELECT ON public.users" in obj.command


def test_parse_multiple_statements():
    """Test parsing multiple statements in one SQL string."""
    sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    
    CREATE INDEX idx_users_name ON users(name);
    
    GRANT SELECT ON users TO app_user;
    """
    
    result = parse_sql_to_ast_objects(sql)
    
    assert len(result) == 3
    
    # Check table
    table_obj = next(obj for obj in result if obj.query_type == BuildStage.BASE_TABLE)
    assert table_obj.object_name == "users"
    
    # Check index
    index_obj = next(obj for obj in result if obj.query_type == BuildStage.INDEX)
    assert index_obj.object_name == "idx_users_name"
    
    # Check grant
    grant_obj = next(obj for obj in result if obj.query_type == BuildStage.GRANT)
    assert grant_obj.object_name == "users"


def test_parse_constraint():
    """Test parsing an ALTER TABLE ADD CONSTRAINT statement."""
    sql = """
    ALTER TABLE users ADD CONSTRAINT chk_name CHECK (length(name) >= 2);
    """
    
    result = parse_sql_to_ast_objects(sql)
    
    print(f"Parsed {len(result)} objects")
    for i, obj in enumerate(result):
        print(f"Object {i}: {obj.query_type.value} - {obj.object_name}")
        print(f"  Command: {obj.command[:100]}...")
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.CONSTRAINT
    assert obj.object_name == "chk_name"
    assert "users" in obj.dependencies
    assert "ALTER TABLE users ADD CONSTRAINT chk_name" in obj.command


def test_parse_policy():
    """Test parsing a CREATE POLICY statement."""
    sql = """
    CREATE POLICY users_select_policy ON users FOR SELECT USING (true);
    """
    
    result = parse_sql_to_ast_objects(sql)
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.POLICY
    assert obj.object_name == "users_select_policy"
    assert "users" in obj.dependencies
    assert "CREATE POLICY users_select_policy" in obj.command


def test_legacy_compatibility():
    """Test that legacy extract_build_queries function still works."""
    sql = """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );
    """
    
    result = extract_build_queries(sql, use_ast_objects=True)
    
    assert len(result) == 1
    obj = result[0]
    assert obj.query_type == BuildStage.BASE_TABLE
    assert obj.object_name == "users" 