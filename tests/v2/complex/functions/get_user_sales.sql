CREATE OR REPLACE FUNCTION complex.get_user_sales(p_user_id INTEGER)
RETURNS TABLE (
    sale_id INTEGER,
    amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    sale_date TIMESTAMP,
    status VARCHAR(20),
    payment_method VARCHAR(50)
) AS $$
BEGIN
    RETURN QUERY
    SELECT s.id, s.amount, s.tax_amount, s.total_amount, s.sale_date, s.status, s.payment_method
    FROM complex.sales s
    WHERE s.user_id = p_user_id
    ORDER BY s.sale_date DESC;
END;
$$ LANGUAGE plpgsql; 