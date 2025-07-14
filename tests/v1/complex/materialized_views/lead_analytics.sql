CREATE MATERIALIZED VIEW complex.lead_analytics AS
SELECT 
    source,
    COUNT(*) as lead_count,
    COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as email_count
FROM complex.leads
GROUP BY source
ORDER BY lead_count DESC; 