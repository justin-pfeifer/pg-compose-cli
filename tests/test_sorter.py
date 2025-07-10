import os
from pg_compose_cli.sorter import sort_queries, sort_alter_commands
from pg_compose_cli.compare import load_source
from pg_compose_cli.ast_objects import ASTList, ASTObject, BuildStage

def test_sort_with_actual_sql_files():
    """Test sorting using actual SQL files from test directory"""
    # Get the path to the test data directory
    test_data_dir = os.path.join(os.path.dirname(__file__), "sort_test_data")
    
    # Load queries from the actual SQL files using the proper directory handling
    queries = load_source(test_data_dir, use_ast_objects=False)
    
    # Sort the queries
    sorted_queries = sort_queries(queries)
    
    # Verify the order
    object_names = [q["object_name"] for q in sorted_queries]
    
    # Should be: users -> orders -> order_items
    assert object_names[0] == "users"
    assert object_names[1] == "orders" 
    assert object_names[2] == "order_items"
    
    # Verify dependencies are satisfied
    seen_objects = set()
    for q in sorted_queries:
        object_name = q['object_name']
        dependencies = q['dependencies']
        
        for dep in dependencies:
            assert dep in seen_objects, f"{object_name} depends on {dep} but {dep} comes after!"
        
        seen_objects.add(object_name)

def test_sort_with_actual_sql_files_ast_objects():
    """Test sorting using actual SQL files with ASTObjects"""
    # Get the path to the test data directory
    test_data_dir = os.path.join(os.path.dirname(__file__), "sort_test_data")
    
    # Load queries from the actual SQL files using ASTObjects
    ast_list = load_source(test_data_dir, use_ast_objects=True)
    
    # Sort the queries using object names
    sorted_ast_list = sort_queries(ast_list, use_object_names=True, grant_handling=True)
    
    # Verify the order
    object_names = [obj.object_name for obj in sorted_ast_list]
    
    # Should be: users -> orders -> order_items
    assert object_names[0] == "users"
    assert object_names[1] == "orders" 
    assert object_names[2] == "order_items"
    
    # Verify dependencies are satisfied
    seen_objects = set()
    for obj in sorted_ast_list:
        object_name = obj.object_name
        dependencies = obj.dependencies
        
        for dep in dependencies:
            assert dep in seen_objects, f"{object_name} depends on {dep} but {dep} comes after!"
        
        seen_objects.add(object_name)

def test_sort_with_complex_dependencies():
    """Test sorting with more complex dependency chains using actual SQL files"""
    # Get the path to the complex test data directory
    test_data_dir = os.path.join(os.path.dirname(__file__), "sort_test_data_complex")
    
    # Load queries from the actual SQL files using the proper directory handling
    queries = load_source(test_data_dir, use_ast_objects=False)
    sorted_queries = sort_queries(queries)
    
    # Verify order: categories, users (no deps) -> products, orders -> order_items
    object_names = [q["object_name"] for q in sorted_queries]
    
    # Verify that objects with no dependencies come first (categories and users)
    # But don't assume specific positions - just check they're before their dependents
    categories_index = object_names.index("categories")
    users_index = object_names.index("users")
    products_index = object_names.index("products")
    orders_index = object_names.index("orders")
    order_items_index = object_names.index("order_items")
    
    # Products should come after categories (its dependency)
    assert products_index > categories_index
    # Orders should come after users (its dependency)
    assert orders_index > users_index
    # Order_items should come after both orders and products (its dependencies)
    assert order_items_index > products_index
    assert order_items_index > orders_index
    
    # Verify dependencies are satisfied
    seen_objects = set()
    for q in sorted_queries:
        object_name = q['object_name']
        dependencies = q['dependencies']
        
        for dep in dependencies:
            assert dep in seen_objects, f"{object_name} depends on {dep} but {dep} comes after!"
        
        seen_objects.add(object_name) 

def test_sort_with_complex_dependencies_ast_objects():
    """Test sorting with more complex dependency chains using ASTObjects"""
    # Get the path to the complex test data directory
    test_data_dir = os.path.join(os.path.dirname(__file__), "sort_test_data_complex")
    
    # Load queries from the actual SQL files using ASTObjects
    ast_list = load_source(test_data_dir, use_ast_objects=True)
    sorted_ast_list = sort_queries(ast_list, use_object_names=True, grant_handling=True)
    
    # Verify order: categories, users (no deps) -> products, orders -> order_items
    object_names = [obj.object_name for obj in sorted_ast_list]
    
    # Verify that objects with no dependencies come first (categories and users)
    # But don't assume specific positions - just check they're before their dependents
    categories_index = object_names.index("categories")
    users_index = object_names.index("users")
    products_index = object_names.index("products")
    orders_index = object_names.index("orders")
    order_items_index = object_names.index("order_items")
    
    # Products should come after categories (its dependency)
    assert products_index > categories_index
    # Orders should come after users (its dependency)
    assert orders_index > users_index
    # Order_items should come after both orders and products (its dependencies)
    assert order_items_index > products_index
    assert order_items_index > orders_index
    
    # Verify dependencies are satisfied
    seen_objects = set()
    for obj in sorted_ast_list:
        object_name = obj.object_name
        dependencies = obj.dependencies
        
        for dep in dependencies:
            assert dep in seen_objects, f"{object_name} depends on {dep} but {dep} comes after!"
        
        seen_objects.add(object_name)

def test_ast_list_sorting():
    """Test ASTList.sort() method directly"""
    # Create ASTObjects with dependencies
    ast_objects = [
        ASTObject(
            command="CREATE TABLE users (id SERIAL PRIMARY KEY);",
            object_name="users",
            query_type=BuildStage.BASE_TABLE,
            dependencies=[]
        ),
        ASTObject(
            command="CREATE TABLE orders (id SERIAL, user_id INTEGER REFERENCES users(id));",
            object_name="orders",
            query_type=BuildStage.DEPENDENT_TABLE,
            dependencies=["users"]
        ),
        ASTObject(
            command="GRANT SELECT ON users TO app_user;",
            object_name="grant_on_users",
            query_type=BuildStage.GRANT,
            dependencies=["users"]
        )
    ]
    
    # Create ASTList and sort
    ast_list = ASTList(ast_objects)
    sorted_list = ast_list.sort()
    
    # Verify order
    object_names = [obj.object_name for obj in sorted_list]
    
    # Users should come first (no dependencies)
    assert object_names[0] == "users"
    
    # Orders and grant_on_users should come after users
    assert "orders" in object_names[1:] or "grant_on_users" in object_names[1:]
    
    # Verify dependencies are satisfied
    seen_objects = set()
    for obj in sorted_list:
        object_name = obj.object_name
        dependencies = obj.dependencies
        
        for dep in dependencies:
            assert dep in seen_objects, f"{object_name} depends on {dep} but {dep} comes after!"
        
        seen_objects.add(object_name)

def test_ast_list_merge_and_sort():
    """Test ASTList merge and sort functionality"""
    # Create two ASTLists
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
        ),
        ASTObject(
            command="GRANT SELECT ON users TO app_user;",
            object_name="grant_on_users",
            query_type=BuildStage.GRANT,
            dependencies=["users"]
        )
    ])
    
    # Merge and sort
    merged = list1.merge(list2)
    sorted_merged = merged.sort()
    
    # Verify we have all objects
    assert len(sorted_merged) == 3, "Should have 3 objects after merge"
    
    # Verify order
    object_names = [obj.object_name for obj in sorted_merged]
    assert "users" in object_names, "Should have users table"
    assert "orders" in object_names, "Should have orders table"
    assert "grant_on_users" in object_names, "Should have grant"
    
    # Users should come first
    assert object_names[0] == "users", "Users should come first (no dependencies)" 