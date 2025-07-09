from pg_compose_cli.compare import compare_sources
import os

def test_compare_users():
    """Test comparing users table between versions."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    source_a = os.path.join(base_dir, "users", "v1.sql")
    source_b = os.path.join(base_dir, "users", "v2.sql")
    result = compare_sources(source_a, source_b, verbose=False)
    # Both files define the same table name 'users', so we expect changes, not create/drop
    assert len(result['created']) == 0, "Should not create new tables"
    assert len(result['dropped']) == 0, "Should not drop tables"
    assert len(result['changed']) == 1, "Should have one changed table"
    assert result['changed'][0]['object_name'] == 'users', "Changed table should be 'users'"

def test_feature_change():
    """Test comparing feature branch changes - products table with new column and dependent view."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    
    # Compare feature_change v1 vs v2
    v1_schema = os.path.join(base_dir, "feature_change", "v1", "public")
    v2_schema = os.path.join(base_dir, "feature_change", "v2", "public")
    
    result = compare_sources(v1_schema, v2_schema, verbose=False)
    
    # Should have changes for products table and dependent view
    assert len(result['changed']) >= 2, "Should have at least 2 changed objects (products table and product_summary view)"
    
    # Check for products table change
    products_change = next((obj for obj in result['changed'] if obj['object_name'] == 'products'), None)
    assert products_change is not None, "Should have products table in changed list"
    
    # Check for product_summary view change (depends on products table)
    product_summary_change = next((obj for obj in result['changed'] if obj['object_name'] == 'product_summary'), None)
    assert product_summary_change is not None, "Should have product_summary view in changed list"
    
    # Should have no created or dropped objects since we're comparing complete schemas
    assert len(result['created']) == 0, "Should not have created objects when comparing complete schemas"
    assert len(result['dropped']) == 0, "Should not have dropped objects when comparing complete schemas"

def test_table_removal():
    """Test table removal scenario - users table removed in v2."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    
    # Compare table_removal v1 vs v2
    v1_schema = os.path.join(base_dir, "table_removal", "v1", "public")
    v2_schema = os.path.join(base_dir, "table_removal", "v2", "public")
    
    result = compare_sources(v1_schema, v2_schema, verbose=False)
    
    # Should have one dropped table (users) and one changed table (orders - foreign key removed)
    assert len(result['dropped']) == 1, "Should have one dropped table (users)"
    assert len(result['changed']) == 1, "Should have one changed table (orders - foreign key removed)"
    
    # Check for users table removal
    users_dropped = next((obj for obj in result['dropped'] if obj['object_name'] == 'users'), None)
    assert users_dropped is not None, "Should have users table in dropped list"
    
    # Check for orders table change (foreign key reference removed)
    orders_change = next((obj for obj in result['changed'] if obj['object_name'] == 'orders'), None)
    assert orders_change is not None, "Should have orders table in changed list"
    
    # Should have no created objects
    assert len(result['created']) == 0, "Should not have created objects"