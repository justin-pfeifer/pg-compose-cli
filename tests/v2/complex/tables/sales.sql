CREATE TABLE complex.sales (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES complex.users(id),
    amount DECIMAL(10,2) NOT NULL,
    tax_amount DECIMAL(10,2) DEFAULT 0.00,
    total_amount DECIMAL(10,2) NOT NULL,
    sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending',
    payment_method VARCHAR(50)
); 