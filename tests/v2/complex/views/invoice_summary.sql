CREATE VIEW complex.invoice_summary AS
SELECT 
    u.username,
    COUNT(i.id) as total_invoices,
    SUM(i.amount) as total_amount,
    SUM(i.tax_amount) as total_tax,
    SUM(i.total_amount) as grand_total
FROM complex.users u
LEFT JOIN complex.invoices i ON u.id = i.user_id
WHERE u.is_active = true
GROUP BY u.id, u.username; 