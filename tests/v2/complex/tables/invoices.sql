CREATE TABLE complex.invoices (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES complex.users(id),
    invoice_number VARCHAR(50) UNIQUE NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    tax_amount DECIMAL(10,2) DEFAULT 0.00,
    total_amount DECIMAL(10,2) NOT NULL,
    due_date DATE,
    status VARCHAR(20) DEFAULT 'unpaid',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions on invoices table
GRANT SELECT ON complex.invoices TO app_user;
GRANT SELECT, INSERT, UPDATE ON complex.invoices TO billing_user;
GRANT SELECT ON complex.invoices TO readonly_user; 