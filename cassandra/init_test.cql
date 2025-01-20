CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH REPLICATION = {'class':'NetworkTopologyStrategy','3266281-df55181':'2'};
USE test_keyspace;
CREATE TABLE IF NOT EXISTS test_table (
    id bigint PRIMARY KEY,
    name text,
    value int
);

BEGIN BATCH 
INSERT INTO test_table (id, name, value) VALUES (1, 'Test 1', 100);
INSERT INTO test_table (id, name, value) VALUES (2, 'Test 2', 200);
INSERT INTO test_table (id, name, value) VALUES (3, 'Test 3', 300);
INSERT INTO test_table (id, name, value) VALUES (4, 'Test 1', 100);
INSERT INTO test_table (id, name, value) VALUES (5, 'Test 2', 200);
INSERT INTO test_table (id, name, value) VALUES (6, 'Test 3', 300);
INSERT INTO test_table (id, name, value) VALUES (7, 'Test 1', 100);
INSERT INTO test_table (id, name, value) VALUES (8, 'Test 2', 200);
INSERT INTO test_table (id, name, value) VALUES (9, 'Test 3', 300);
APPLY BATCH;

SELECT * FROM test_table;
