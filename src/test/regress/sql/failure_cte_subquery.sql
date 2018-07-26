
CREATE SCHEMA cte_failure;
SET SEARCH_PATH=cte_failure;
SET citus.shard_count to 8;

SELECT pg_backend_pid() as pid \gset

CREATE TABLE users_table (user_id int, user_name text);
CREATE TABLE events_table(user_id int, event_id int, event_type int);
CREATE TABLE events_rollup(user_id int, event_id int, count int);
SELECT create_distributed_table('users_table', 'user_id');
SELECT create_distributed_table('events_table', 'user_id');
SELECT create_distributed_table('events_rollup', 'user_id');
CREATE TABLE users_table_local AS SELECT * FROM users_table;

WITH cte AS (
	WITH local_cte AS (
		SELECT * FROM users_table_local
	),
	dist_cte AS (
		SELECT user_id FROM events_table
	)
	SELECT dist_cte.user_id FROM local_cte join dist_cte on dist_cte.user_id=local_cte.user_id
)
SELECT 
	count(*) 
FROM 
	cte,
	  (SELECT 
    	DISTINCT users_table.user_id 
     FROM 
     	users_table, events_table 
     WHERE 
     	users_table.user_id = events_table.user_id AND 
     event_type IN (1,2,3,4)
     ORDER BY 1 DESC LIMIT 5
     ) as foo 
	  WHERE foo.user_id = cte.user_id;



insert into events_rollup select user_id, event_id, count(*) from events_table group by 1, 2;

INSERT INTO users_table VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E');


WITH cte_delete as (DELETE FROM users_table WHERE user_name in ('A', 'D') RETURNING *)
insert into users_table select * from cte_delete;
SELECT citus.mitmproxy('conn.allow()');


SELECT * FROM users_table;
SELECT citus.mitmproxy('conn.onQuery(query="^INSERT INTO").cancel(' || :pid || ')');
SELECT citus.clear_network_traffic();


BEGIN;
SAVEPOINT s1;
DELETE FROM users_table;
SAVEPOINT s2;
INSERT INTO users_table VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), (5, 'E');
SAVEPOINT s3;
ROLLBACK TO SAVEPOINT s2;
SELECT * FROM users_table;
ROLLBACK TO SAVEPOINT s1;
SELECT * FROM users_table;

COMMIT;

SELECT * FROM users_table;

SELECT citus.dump_network_traffic();
RESET SEARCH_PATH;
DROP SCHEMA cte_failure CASCADE;


