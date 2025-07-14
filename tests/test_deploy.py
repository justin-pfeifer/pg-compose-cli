import pytest
import tempfile
import os
from pg_compose_core.lib.deploy import diff_sort, deploy
from pg_compose_core.lib.ast import ASTList


class TestDeploy:
    """Test deployment functionality independent of CLI."""
    
    def test_diff_sort_new_table(self):
        """Test diff_sort with a new table."""
        source_a = """
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        """
        
        source_b = """
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            amount DECIMAL(10,2)
        );
        """
        
        # Write sources to temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f1:
            f1.write(source_a)
            file_a = f1.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f2:
            f2.write(source_b)
            file_b = f2.name
        
        try:
            result = diff_sort(file_a, file_b)
            
            # Should have one alter command for the new table
            assert len(result) == 1
            assert result[0].query_type.value == "base_table"
            assert result[0].object_name == "orders"
            
        finally:
            os.unlink(file_a)
            os.unlink(file_b)
    
    def test_diff_sort_qualified_names(self):
        """Test diff_sort with schema-qualified names."""
        source_a = """
        CREATE TABLE public.users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        """
        
        source_b = """
        CREATE TABLE public.users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        CREATE TABLE public.orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES public.users(id),
            amount DECIMAL(10,2)
        );
        """
        
        # Write sources to temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f1:
            f1.write(source_a)
            file_a = f1.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f2:
            f2.write(source_b)
            file_b = f2.name
        
        try:
            result = diff_sort(file_a, file_b)
            
            # Should have one alter command for the new table
            assert len(result) == 1
            assert result[0].query_type.value == "base_table"
            assert result[0].qualified_name == "public.orders"
            
        finally:
            os.unlink(file_a)
            os.unlink(file_b)
    
    def test_diff_sort_new_column(self):
        """Test diff_sort with a new column."""
        source_a = """
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        """
        
        source_b = """
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(255)
        );
        """
        
        # Write sources to temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f1:
            f1.write(source_a)
            file_a = f1.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f2:
            f2.write(source_b)
            file_b = f2.name
        
        try:
            result = diff_sort(file_a, file_b)
            
            # Should have one alter command for the new column
            assert len(result) == 1
            assert result[0].query_type.value == "unknown"
            
        finally:
            os.unlink(file_a)
            os.unlink(file_b)
    
    def test_diff_sort_new_index(self):
        """Test diff_sort with a new index."""
        source_a = """
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        """
        
        source_b = """
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        CREATE INDEX idx_users_name ON users(name);
        """
        
        # Write sources to temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f1:
            f1.write(source_a)
            file_a = f1.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f2:
            f2.write(source_b)
            file_b = f2.name
        
        try:
            result = diff_sort(file_a, file_b)
            
            # Should have one alter command for the new index
            assert len(result) == 1
            assert result[0].query_type.value == "index"
            assert result[0].object_name == "idx_users_name"
            
        finally:
            os.unlink(file_a)
            os.unlink(file_b)
    
    def test_diff_sort_no_changes(self):
        """Test diff_sort with no changes."""
        source = """
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        """
        
        # Write source to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(source)
            file_path = f.name
        
        try:
            result = diff_sort(file_path, file_path)
            
            # Should have no changes
            assert len(result) == 0
            
        finally:
            os.unlink(file_path)
    
    def test_deploy_dry_run(self):
        """Test deploy function in dry-run mode."""
        # Create a simple ASTList for testing
        from pg_compose_core.lib.ast_objects import ASTObject, BuildStage
        
        test_objects = [
            ASTObject(
                command="CREATE TABLE test (id SERIAL PRIMARY KEY)",
                object_name="test",
                query_type=BuildStage.BASE_TABLE,
                dependencies=[],
                query_hash="test_hash_1"
            )
        ]
        
        ast_list = ASTList(test_objects)
        
        # Test dry run
        result = deploy(ast_list, "test_output.sql", dry_run=True, verbose=True)
        
        assert result["status"] == "preview"
        assert result["target"] == "test_output.sql"
        assert result["changes_count"] == 1
        assert "CREATE TABLE test" in result["sql"]
        assert "Dry run completed" in result["message"]
    
    def test_deploy_sql_string(self):
        """Test deploy function with SQL string input."""
        sql = "CREATE TABLE test (id SERIAL PRIMARY KEY);"
        
        # Test dry run with SQL string
        result = deploy(sql, "test_output.sql", dry_run=True, verbose=False)
        
        assert result["status"] == "preview"
        assert result["target"] == "test_output.sql"
        assert result["changes_count"] == 1
        assert sql in result["sql"]
    
    def test_diff_sort_with_grants(self):
        """Test diff_sort with grant statements."""
        source_a = """
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        """
        
        source_b = """
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        GRANT SELECT, INSERT ON users TO app_user;
        """
        
        # Write sources to temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f1:
            f1.write(source_a)
            file_a = f1.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f2:
            f2.write(source_b)
            file_b = f2.name
        
        try:
            result = diff_sort(file_a, file_b, grants=True)
            
            # Should have one grant command
            assert len(result) == 1
            assert result[0].query_type.value == "grant"
            assert result[0].object_name == "users"
            assert result[0].resource_type.value == "table"
            
        finally:
            os.unlink(file_a)
            os.unlink(file_b)
    
    def test_diff_sort_without_grants(self):
        """Test diff_sort without grant statements."""
        source_a = """
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        """
        
        source_b = """
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
        GRANT SELECT, INSERT ON users TO app_user;
        """
        
        # Write sources to temporary files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f1:
            f1.write(source_a)
            file_a = f1.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f2:
            f2.write(source_b)
            file_b = f2.name
        
        try:
            result = diff_sort(file_a, file_b, grants=False)
            
            # Should have no changes since grants are excluded
            assert len(result) == 0
            
        finally:
            os.unlink(file_a)
            os.unlink(file_b) 