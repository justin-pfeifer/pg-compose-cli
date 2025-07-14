CREATE OR REPLACE FUNCTION complex.process_invoices()
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM complex.invoices
    WHERE status = 'unpaid' AND due_date < CURRENT_DATE;
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql; 