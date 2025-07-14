CREATE OR REPLACE FUNCTION complex.calculate_total(p_amount DECIMAL(10,2), p_tax_rate DECIMAL(5,4) DEFAULT 0.08)
RETURNS DECIMAL(10,2) AS $$
DECLARE
    v_tax_amount DECIMAL(10,2);
    v_total DECIMAL(10,2);
BEGIN
    v_tax_amount := p_amount * p_tax_rate;
    v_total := p_amount + v_tax_amount;
    
    RETURN ROUND(v_total, 2);
END;
$$ LANGUAGE plpgsql; 