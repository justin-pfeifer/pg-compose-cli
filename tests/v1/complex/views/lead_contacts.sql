CREATE VIEW complex.lead_contacts AS
SELECT name, email, phone, source
FROM complex.leads
WHERE email IS NOT NULL; 