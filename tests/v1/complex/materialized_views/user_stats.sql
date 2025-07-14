CREATE MATERIALIZED VIEW complex.user_stats AS
SELECT 
    u.id,
    u.username,
    COUNT(s.id) as sales_count,
    SUM(s.amount) as total_sales
FROM complex.users u
LEFT JOIN complex.sales s ON u.id = s.user_id
WHERE u.is_active = true
GROUP BY u.id, u.username
ORDER BY total_sales DESC; 