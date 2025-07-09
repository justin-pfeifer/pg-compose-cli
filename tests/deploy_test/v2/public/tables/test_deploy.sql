CREATE TABLE test_deploy (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    price NUMERIC(12,4), -- changed precision
    status my_schema.status_type DEFAULT 'inactive', -- changed default
    description VARCHAR(50) NOT NULL DEFAULT '', -- new column
    -- notes column removed
    extra_col INT
); 