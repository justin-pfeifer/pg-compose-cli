CREATE TABLE complex.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(20),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions on users table
GRANT SELECT ON complex.users TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON complex.users TO admin_user;
GRANT SELECT ON complex.users TO readonly_user; 