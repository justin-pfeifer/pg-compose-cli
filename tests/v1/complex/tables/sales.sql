CREATE TABLE complex.sales (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES complex.users(id),
    amount DECIMAL(10,2) NOT NULL,
    sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending'
);

-- Grant permissions on sales table
GRANT SELECT ON complex.sales TO app_user;
GRANT SELECT, INSERT, UPDATE ON complex.sales TO sales_user; 