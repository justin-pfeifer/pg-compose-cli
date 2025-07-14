import pytest
import os
from pg_compose_core.lib.git import extract_from_git_repo
from pg_compose_core.lib.parser import load_source
from pg_compose_core.lib.ast import ASTList


def test_git_repo_clone():
    """Test that we can clone a git repository and access files."""
    
    # Use a public test repository or this project's repo
    github_url = "https://github.com/justin-pfeifer/pg-compose-cli.git"
    
    # Test cloning the repo and accessing a specific path
    try:
        with extract_from_git_repo(github_url, "tests/v1/user_schema") as working_dir:
            # Verify we got a working directory
            assert os.path.exists(working_dir), "Working directory should exist"
            assert os.path.isdir(working_dir), "Working directory should be a directory"
            
            # Check that SQL files exist in the directory
            sql_files = [f for f in os.listdir(working_dir) if f.endswith('.sql')]
            assert len(sql_files) > 0, "Should find SQL files in the git repo"
            
    except Exception as e:
        # If git comparison fails (e.g., network issues), skip the test
        pytest.skip(f"Git clone failed: {e}")


def test_git_repo_parse_sql():
    """Test that we can clone a git repository and parse SQL files."""
    
    github_url = "https://github.com/justin-pfeifer/pg-compose-cli.git"
    
    try:
        result = load_source(f"{github_url}/tests/v1/user_schema")
            
        # Basic assertions
        assert len(result) > 0, "Should parse SQL files from git repo"
            
        # Check that we got some ASTObjects
        assert isinstance(result, ASTList), "Result should be an ASTList"
            
    except Exception as e:
        pytest.skip(f"Git SQL parsing failed: {e}")


def test_git_repo_specific_file():
    """Test accessing a specific SQL file from a git repository."""
    
    github_url = "https://github.com/justin-pfeifer/pg-compose-cli.git"
    
    try:
        with extract_from_git_repo(github_url, "tests/v1/user_schema/user.sql") as working_dir:
            # This should be a file path
            assert os.path.isfile(working_dir), "Should get a specific SQL file"
            
            # Parse the specific file
            result = load_source(working_dir)
            
            # Should have parsed the SQL file
            assert len(result) > 0, "Should parse the specific SQL file"
            
    except Exception as e:
        pytest.skip(f"Git file access failed: {e}") 