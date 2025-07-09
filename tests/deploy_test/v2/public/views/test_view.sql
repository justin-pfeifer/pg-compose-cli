CREATE VIEW test_view AS
SELECT id, name, description FROM test_deploy WHERE status = 'inactive'; 