CREATE OR REPLACE FUNCTION complex.get_user_sales(p_user_id INTEGER)
RETURNS TABLE (
    sale_id INTEGER,
    amount DECIMAL(10,2),
    sale_date TIMESTAMP,
    status VARCHAR(20)
) AS $$
BEGIN
    RETURN QUERY
    SELECT s.id, s.amount, s.sale_date, s.status
    FROM complex.sales s
    WHERE s.user_id = p_user_id
    ORDER BY s.sale_date DESC;
END;
$$ LANGUAGE plpgsql; 