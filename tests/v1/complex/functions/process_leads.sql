CREATE OR REPLACE FUNCTION complex.process_leads()
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM complex.leads
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days';
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql; 