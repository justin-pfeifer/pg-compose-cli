CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_name TEXT,
    quantity INTEGER,
    unit_price DECIMAL(10,2)
); 