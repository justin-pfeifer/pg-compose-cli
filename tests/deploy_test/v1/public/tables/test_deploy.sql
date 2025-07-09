CREATE TABLE test_deploy (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    price NUMERIC(10,2),
    status my_schema.status_type DEFAULT 'active',
    notes TEXT
); 