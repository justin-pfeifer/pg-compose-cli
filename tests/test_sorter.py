import os
from pg_compose_core.lib.compare import load_source
from pg_compose_core.lib.ast_objects import ASTList

def test_sort_with_actual_sql_files():
    """Test sorting with actual SQL files from test data."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    test_data_dir = os.path.join(base_dir, "sort_test_data")
    queries = load_source(test_data_dir)
    # Convert to ASTList for sorting
    ast_list = ASTList(queries)
    sorted_queries = ast_list.sort()
    sql = sorted_queries.to_sql()
    
    # Check that all tables are present
    assert "CREATE TABLE users" in sql
    assert "CREATE TABLE orders" in sql
    assert "CREATE TABLE order_items" in sql
    
    # Check dependency order
    users_idx = sql.find("CREATE TABLE users")
    orders_idx = sql.find("CREATE TABLE orders")
    order_items_idx = sql.find("CREATE TABLE order_items")
    
    # Users should come before orders (orders depends on users)
    assert users_idx < orders_idx
    # Orders should come before order_items (order_items depends on orders)
    assert orders_idx < order_items_idx

def test_sort_with_actual_sql_files_ast_objects():
    """Test sorting with actual SQL files using ASTObjects."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    test_data_dir = os.path.join(base_dir, "sort_test_data")
    ast_list = load_source(test_data_dir)
    sorted_queries = ast_list.sort()
    sql = sorted_queries.to_sql()
    
    # Check that all tables are present
    assert "CREATE TABLE users" in sql
    assert "CREATE TABLE orders" in sql
    assert "CREATE TABLE order_items" in sql
    
    # Check dependency order
    users_idx = sql.find("CREATE TABLE users")
    orders_idx = sql.find("CREATE TABLE orders")
    order_items_idx = sql.find("CREATE TABLE order_items")
    
    # Users should come before orders (orders depends on users)
    assert users_idx < orders_idx
    # Orders should come before order_items (order_items depends on orders)
    assert orders_idx < order_items_idx

def test_sort_with_complex_dependencies():
    """Test sorting with more complex dependency scenarios."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    test_data_dir = os.path.join(base_dir, "sort_test_data_complex")
    queries = load_source(test_data_dir)
    # Convert to ASTList for sorting
    ast_list = ASTList(queries)
    sorted_queries = ast_list.sort()
    sql = sorted_queries.to_sql()
    
    # Check that all tables are present
    assert "CREATE TABLE categories" in sql
    assert "CREATE TABLE products" in sql
    assert "CREATE TABLE users" in sql
    assert "CREATE TABLE orders" in sql
    assert "CREATE TABLE order_items" in sql
    
    # Check dependency order
    categories_idx = sql.find("CREATE TABLE categories")
    products_idx = sql.find("CREATE TABLE products")
    users_idx = sql.find("CREATE TABLE users")
    orders_idx = sql.find("CREATE TABLE orders")
    order_items_idx = sql.find("CREATE TABLE order_items")
    
    # Categories should come before products (products depends on categories)
    assert categories_idx < products_idx
    # Users should come before orders (orders depends on users)
    assert users_idx < orders_idx
    # Products and orders should come before order_items (order_items depends on both)
    assert products_idx < order_items_idx
    assert orders_idx < order_items_idx

def test_sort_with_complex_dependencies_ast_objects():
    """Test sorting with complex dependencies using ASTObjects."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    test_data_dir = os.path.join(base_dir, "sort_test_data_complex")
    ast_list = load_source(test_data_dir)
    sorted_queries = ast_list.sort()
    sql = sorted_queries.to_sql()
    
    # Check that all tables are present
    assert "CREATE TABLE categories" in sql
    assert "CREATE TABLE products" in sql
    assert "CREATE TABLE users" in sql
    assert "CREATE TABLE orders" in sql
    assert "CREATE TABLE order_items" in sql
    
    # Check dependency order
    categories_idx = sql.find("CREATE TABLE categories")
    products_idx = sql.find("CREATE TABLE products")
    users_idx = sql.find("CREATE TABLE users")
    orders_idx = sql.find("CREATE TABLE orders")
    order_items_idx = sql.find("CREATE TABLE order_items")
    
    # Categories should come before products (products depends on categories)
    assert categories_idx < products_idx
    # Users should come before orders (orders depends on users)
    assert users_idx < orders_idx
    # Products and orders should come before order_items (order_items depends on both)
    assert products_idx < order_items_idx
    assert orders_idx < order_items_idx

def test_ast_list_sorting():
    """Test ASTList.sort() method directly."""
    from pg_compose_core.lib.ast_objects import ASTObject, BuildStage
    
    # Create test objects with dependencies
    users = ASTObject(
        command="CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);",
        object_name="users",
        query_type=BuildStage.BASE_TABLE,
        dependencies=[]
    )
    
    orders = ASTObject(
        command="CREATE TABLE orders (id SERIAL, user_id INTEGER REFERENCES users(id));",
        object_name="orders",
        query_type=BuildStage.DEPENDENT_TABLE,
        dependencies=["users"]
    )
    
    order_items = ASTObject(
        command="CREATE TABLE order_items (id SERIAL, order_id INTEGER REFERENCES orders(id));",
        object_name="order_items",
        query_type=BuildStage.DEPENDENT_TABLE,
        dependencies=["orders"]
    )
    
    # Create ASTList and sort
    ast_list = ASTList([order_items, users, orders])  # Wrong order
    sorted_list = ast_list.sort()
    
    # Check order
    assert sorted_list[0].object_name == "users"
    assert sorted_list[1].object_name == "orders"
    assert sorted_list[2].object_name == "order_items"

def test_ast_list_merge_and_sort():
    """Test ASTList.merge() and sort() methods together."""
    from pg_compose_core.lib.ast_objects import ASTObject, BuildStage
    
    # Create two separate ASTLists
    list1 = ASTList([
        ASTObject(
            command="CREATE TABLE users (id SERIAL PRIMARY KEY);",
            object_name="users",
            query_type=BuildStage.BASE_TABLE,
            dependencies=[]
        )
    ])
    
    list2 = ASTList([
        ASTObject(
            command="CREATE TABLE orders (id SERIAL, user_id INTEGER REFERENCES users(id));",
            object_name="orders",
            query_type=BuildStage.DEPENDENT_TABLE,
            dependencies=["users"]
        )
    ])
    
    # Merge and sort
    merged = list1.merge(list2)
    sorted_merged = merged.sort()
    
    # Check results
    assert len(sorted_merged) == 2
    assert sorted_merged[0].object_name == "users"
    assert sorted_merged[1].object_name == "orders" 