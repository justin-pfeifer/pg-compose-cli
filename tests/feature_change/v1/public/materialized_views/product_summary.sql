CREATE MATERIALIZED VIEW product_summary AS
        SELECT id, name FROM products WHERE price > 100;