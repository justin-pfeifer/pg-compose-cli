CREATE VIEW test_view AS
SELECT id, name FROM test_deploy WHERE status = 'active'; 