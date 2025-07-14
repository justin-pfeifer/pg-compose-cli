CREATE MATERIALIZED VIEW complex.invoice_analytics AS
SELECT 
    status,
    COUNT(*) as invoice_count,
    SUM(amount) as total_amount,
    SUM(tax_amount) as total_tax,
    SUM(total_amount) as grand_total
FROM complex.invoices
GROUP BY status
ORDER BY grand_total DESC; 