import os
from pg_compose_cli.sorter import sort_queries
from pg_compose_cli.compare import load_source

def test_sort_with_actual_sql_files():
    """Test sorting using actual SQL files from test directory"""
    # Get the path to the test data directory
    test_data_dir = os.path.join(os.path.dirname(__file__), "sort_test_data")
    
    # Load queries from the actual SQL files using the proper directory handling
    queries = load_source(test_data_dir)
    
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

def test_sort_with_complex_dependencies():
    """Test sorting with more complex dependency chains using actual SQL files"""
    # Get the path to the complex test data directory
    test_data_dir = os.path.join(os.path.dirname(__file__), "sort_test_data_complex")
    
    # Load queries from the actual SQL files using the proper directory handling
    queries = load_source(test_data_dir)
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