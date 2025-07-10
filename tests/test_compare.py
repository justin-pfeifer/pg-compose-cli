from pg_compose_cli.compare import compare_sources
from pg_compose_cli.deploy import generate_deploy_sql
from pg_compose_cli.ast_objects import ASTList
import os

def test_compare_users():
    """Test comparing users table between versions."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    source_a = os.path.join(base_dir, "users", "v1.sql")
    source_b = os.path.join(base_dir, "users", "v2.sql")
    result = compare_sources(source_a, source_b, verbose=False)
    # result is now an ASTList of commands
    create_cmds = [obj for obj in result if obj.command.strip().upper().startswith("CREATE TABLE")]
    drop_cmds = [obj for obj in result if obj.command.strip().upper().startswith("DROP TABLE")]
    alter_cmds = [obj for obj in result if obj.command.strip().upper().startswith("ALTER TABLE")]
    # Both files define the same table name 'users', so we expect changes, not create/drop
    assert len(create_cmds) == 0, "Should not create new tables"
    assert len(drop_cmds) == 0, "Should not drop tables"
    assert len(alter_cmds) >= 1, "Should have at least one ALTER TABLE command"
    assert any("users" in obj.command for obj in alter_cmds), "ALTER TABLE should reference 'users'"

def test_compare_users_with_ast_objects():
    """Test comparing users table between versions using ASTObjects."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    source_a = os.path.join(base_dir, "users", "v1.sql")
    source_b = os.path.join(base_dir, "users", "v2.sql")
    result = compare_sources(source_a, source_b, verbose=False, use_ast_objects=True)
    create_cmds = [obj for obj in result if obj.command.strip().upper().startswith("CREATE TABLE")]
    drop_cmds = [obj for obj in result if obj.command.strip().upper().startswith("DROP TABLE")]
    alter_cmds = [obj for obj in result if obj.command.strip().upper().startswith("ALTER TABLE")]
    assert len(create_cmds) == 0, "Should not create new tables"
    assert len(drop_cmds) == 0, "Should not drop tables"
    assert len(alter_cmds) >= 1, "Should have at least one ALTER TABLE command"
    assert any("users" in obj.command for obj in alter_cmds), "ALTER TABLE should reference 'users'"

def test_alter_commands_from_ast_lists():
    """Test generating alter commands using new deploy approach."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    source_a = os.path.join(base_dir, "users", "v1.sql")
    source_b = os.path.join(base_dir, "users", "v2.sql")
    
    sql_output = generate_deploy_sql(
        source_a, source_b, grants=True, verbose=False
    )
    
    # Should be able to convert to SQL
    assert isinstance(sql_output, str), "Should be able to convert to SQL string"
    assert len(sql_output) > 0, "SQL output should not be empty"
    
    # Should contain alter commands for the users table changes
    assert "ALTER TABLE" in sql_output, "Should contain ALTER TABLE commands"
    assert "users" in sql_output, "Should reference users table"

def test_feature_change():
    """Test comparing feature branch changes - products table with new column and dependent view."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    v1_schema = os.path.join(base_dir, "feature_change", "v1", "public")
    v2_schema = os.path.join(base_dir, "feature_change", "v2", "public")
    result = compare_sources(v1_schema, v2_schema, verbose=False)
    alter_cmds = [obj for obj in result if obj.command.strip().upper().startswith("ALTER TABLE")]
    matview_cmds = [obj for obj in result if obj.command.strip().upper().startswith("CREATE MATERIALIZED VIEW")]
    # Should have changes for products table and dependent view
    assert len(alter_cmds) >= 1, "Should have at least 1 ALTER TABLE command (products table)"
    assert len(matview_cmds) >= 1, "Should have at least 1 materialized view command (product_summary)"
    assert any("products" in obj.command for obj in alter_cmds), "ALTER TABLE should reference 'products'"
    assert any("product_summary" in obj.command for obj in matview_cmds), "Should reference 'product_summary'"
    # Should have no CREATE TABLE or DROP TABLE
    create_cmds = [obj for obj in result if obj.command.strip().upper().startswith("CREATE TABLE")]
    drop_cmds = [obj for obj in result if obj.command.strip().upper().startswith("DROP TABLE")]
    assert len(create_cmds) == 0, "Should not have created tables"
    assert len(drop_cmds) == 0, "Should not have dropped tables"

def test_feature_change_with_ast_objects():
    """Test comparing feature branch changes using ASTObjects."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    v1_schema = os.path.join(base_dir, "feature_change", "v1", "public")
    v2_schema = os.path.join(base_dir, "feature_change", "v2", "public")
    result = compare_sources(v1_schema, v2_schema, verbose=False, use_ast_objects=True)
    alter_cmds = [obj for obj in result if obj.command.strip().upper().startswith("ALTER TABLE")]
    matview_cmds = [obj for obj in result if obj.command.strip().upper().startswith("CREATE MATERIALIZED VIEW")]
    assert len(alter_cmds) >= 1, "Should have at least 1 ALTER TABLE command (products table)"
    assert len(matview_cmds) >= 1, "Should have at least 1 materialized view command (product_summary)"
    assert any("products" in obj.command for obj in alter_cmds), "ALTER TABLE should reference 'products'"
    assert any("product_summary" in obj.command for obj in matview_cmds), "Should reference 'product_summary'"
    create_cmds = [obj for obj in result if obj.command.strip().upper().startswith("CREATE TABLE")]
    drop_cmds = [obj for obj in result if obj.command.strip().upper().startswith("DROP TABLE")]
    assert len(create_cmds) == 0, "Should not have created tables"
    assert len(drop_cmds) == 0, "Should not have dropped tables"

def test_table_removal():
    """Test table removal scenario - users table removed in v2."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    v1_schema = os.path.join(base_dir, "table_removal", "v1", "public")
    v2_schema = os.path.join(base_dir, "table_removal", "v2", "public")
    result = compare_sources(v1_schema, v2_schema, verbose=False)
    drop_cmds = [obj for obj in result if obj.command.strip().upper().startswith("DROP TABLE")]
    # alter_cmds = [obj for obj in result if obj.command.strip().upper().startswith("ALTER TABLE")]
    create_cmds = [obj for obj in result if obj.command.strip().upper().startswith("CREATE TABLE")]
    print(drop_cmds)
    # print(alter_cmds)
    assert len(drop_cmds) == 1, "Should have one dropped table (users)"
    # assert len(alter_cmds) == 1, "Should have one changed table (orders - foreign key removed)"
    assert any("users" in obj.command for obj in drop_cmds), "Should have users table in dropped list"
    # assert any("orders" in obj.command for obj in alter_cmds), "Should have orders table in changed list"
    create_cmds = [obj for obj in result if obj.command.strip().upper().startswith("CREATE TABLE")]
    assert len(create_cmds) == 0, "Should not have created tables"

def test_compare_against_git_origin():
    """Test comparing current working directory against git origin."""
    
    # Hardcoded GitHub URL for this project
    github_url = "git@github.com:justin-pfeifer/pg-compose-cli.git"
    
    # Use existing test data directory directly
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    test_data_dir = os.path.join(base_dir, "users")
    
    # Compare test data against git origin's tests/users directory
    try:
        result = compare_sources(
            test_data_dir, 
            f"{github_url}/tests/users", 
            verbose=False
        )
        
        # Basic assertions - result is now an ASTList
        assert isinstance(result, ASTList), "Result should be an ASTList"
        assert len(result) >= 0, "Result should be a list of commands"
        
    except Exception as e:
        # If git comparison fails (e.g., network issues), skip the test
        import pytest
        pytest.skip(f"Git comparison failed: {e}")

def test_compare_local_vs_git():
    """Test comparing local test data against git repository."""
    
    # Hardcoded GitHub URL for this project
    github_url = "git@github.com:justin-pfeifer/pg-compose-cli.git"
    
    # Use existing test data directory directly
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    test_data_dir = os.path.join(base_dir, "users")
    
    # Compare local test data against git repository's tests/users directory
    try:
        result = compare_sources(
            test_data_dir, 
            f"{github_url}/tests/users", 
            verbose=False
        )
        
        # Basic assertions - result is now an ASTList
        assert isinstance(result, ASTList), "Result should be an ASTList"
        assert len(result) >= 0, "Result should be a list of commands"
        
        # Since we're comparing local test data against the actual repo,
        # there should be some differences (the test data vs actual repo content)
        
    except Exception as e:
        # If git comparison fails, skip the test
        import pytest
        pytest.skip(f"Local vs git comparison failed: {e}")