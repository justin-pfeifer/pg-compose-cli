CREATE MATERIALIZED VIEW complex.monthly_sales AS
SELECT 
    DATE_TRUNC('month', sale_date) as month,
    COUNT(*) as total_sales,
    SUM(amount) as total_amount,
    SUM(tax_amount) as total_tax,
    SUM(total_amount) as grand_total
FROM complex.sales
GROUP BY DATE_TRUNC('month', sale_date)
ORDER BY month DESC; 