CREATE VIEW complex.sales_summary AS
SELECT 
    u.username,
    COUNT(s.id) as total_sales,
    SUM(s.amount) as total_amount
FROM complex.users u
LEFT JOIN complex.sales s ON u.id = s.user_id
WHERE u.is_active = true
GROUP BY u.id, u.username; 