CREATE MATERIALIZED VIEW complex.user_stats AS
SELECT 
    u.id,
    u.username,
    COUNT(s.id) as sales_count,
    SUM(s.amount) as total_sales,
    SUM(s.tax_amount) as total_tax,
    SUM(s.total_amount) as grand_total
FROM complex.users u
LEFT JOIN complex.sales s ON u.id = s.user_id
WHERE u.is_active = true
GROUP BY u.id, u.username
ORDER BY grand_total DESC; 