CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT,
    total NUMERIC NOT NULL
); 