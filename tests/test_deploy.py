import os
from pg_compose_core.lib.ast_objects import ASTObject, ASTList, BuildStage
from pg_compose_core.lib.deploy import diff_sort
from pg_compose_core.lib.compare import load_source

def test_deploy_single_table():
    obj = ASTObject(
        command="CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);",
        object_name="users",
        query_type=BuildStage.BASE_TABLE,
        dependencies=[]
    )
    ast_list = ASTList([obj])
    sql = ast_list.to_sql()
    assert "CREATE TABLE users" in sql
    assert "id SERIAL PRIMARY KEY" in sql
    assert "name TEXT" in sql

def test_deploy_multiple_objects_with_dependencies():
    objs = [
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
            command="CREATE VIEW user_orders AS SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id;",
            object_name="user_orders",
            query_type=BuildStage.VIEW,
            dependencies=["users", "orders"]
        ),
        ASTObject(
            command="GRANT SELECT ON users TO app_user;",
            object_name="grant_on_users",
            query_type=BuildStage.GRANT,
            dependencies=["users"]
        )
    ]
    ast_list = ASTList(objs)
    sql = ast_list.sort().to_sql()
    
    # Check that all objects are present
    assert "CREATE TABLE users" in sql
    assert "CREATE TABLE orders" in sql
    assert "CREATE VIEW user_orders" in sql
    assert "GRANT SELECT ON users" in sql
    
    # Check that dependencies are created before they are referenced
    users_idx = sql.find("CREATE TABLE users")
    orders_idx = sql.find("CREATE TABLE orders")
    view_idx = sql.find("CREATE VIEW user_orders")
    grant_idx = sql.find("GRANT SELECT ON users")
    
    # Users must come before orders (orders depends on users)
    assert users_idx < orders_idx
    # Orders must come before view (view depends on orders)
    assert orders_idx < view_idx
    # Users must come before grant (grant depends on users)
    assert users_idx < grant_idx

def test_deploy_alter_scenario_rewrite():
    # Use the users v1/v2 test files
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    v1 = os.path.join(base_dir, "users", "v1.sql")
    v2 = os.path.join(base_dir, "users", "v2.sql")
    result = diff_sort(v1, v2, grants=True)
    sql = result.to_sql()
    print("\n==== GENERATED SQL ====")
    print(sql)
    print("==== END GENERATED SQL ====")
    # Should contain ALTER TABLE users
    assert "ALTER TABLE" in sql or "CREATE TABLE" in sql
    # Should contain the removed column (uid)
    assert "uid" in sql
    # Should contain the new column (phone)
    assert "phone" in sql

def test_deploy_directory():
    # Use the sort_test_data directory
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    test_data_dir = os.path.join(base_dir, "sort_test_data")
    ast_list = load_source(test_data_dir)
    sql = ast_list.sort().to_sql()
    # Should contain all three tables in dependency order
    users_idx = sql.find("CREATE TABLE users")
    orders_idx = sql.find("CREATE TABLE orders")
    order_items_idx = sql.find("CREATE TABLE order_items")
    assert users_idx != -1 and orders_idx != -1 and order_items_idx != -1
    assert users_idx < orders_idx < order_items_idx 