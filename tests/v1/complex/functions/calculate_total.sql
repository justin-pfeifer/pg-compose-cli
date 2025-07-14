CREATE OR REPLACE FUNCTION complex.calculate_total(p_amount DECIMAL(10,2))
RETURNS DECIMAL(10,2) AS $$
BEGIN
    RETURN p_amount;
END;
$$ LANGUAGE plpgsql; 