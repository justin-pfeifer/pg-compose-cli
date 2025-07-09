CREATE MATERIALIZED VIEW product_summary AS
        SELECT id, name, category FROM products WHERE price > 100;