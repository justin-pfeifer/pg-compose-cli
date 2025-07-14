CREATE VIEW complex.active_users AS
SELECT id, username, email, first_name, last_name
FROM complex.users
WHERE is_active = true; 