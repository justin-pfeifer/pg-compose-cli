CREATE VIEW complex.sales_summary AS
SELECT 
    u.username,
    COUNT(s.id) as total_sales,
    SUM(s.amount) as total_amount,
    SUM(s.tax_amount) as total_tax,
    SUM(s.total_amount) as grand_total
FROM complex.users u
LEFT JOIN complex.sales s ON u.id = s.user_id
WHERE u.is_active = true
GROUP BY u.id, u.username; 