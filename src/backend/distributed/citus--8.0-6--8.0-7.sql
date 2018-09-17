/* citus--8.0-6--8.0-7 */
SET search_path = 'pg_catalog';

CREATE VIEW citus.citus_lock_waits AS

WITH citus_dist_stat_activity AS
(
  SELECT * FROM citus_dist_stat_activity
),
citus_dist_stat_activity_with_node_id AS
(
  SELECT 
  citus_dist_stat_activity.*, (CASE citus_dist_stat_activity.master_query_host_name WHEN 'coordinator_host' THEN 0 ELSE pg_dist_node.nodeid END) as initiator_node_id
  FROM
  citus_dist_stat_activity LEFT JOIN pg_dist_node
  ON
  citus_dist_stat_activity.master_query_host_name = pg_dist_node.nodename AND 
  citus_dist_stat_activity.master_query_host_port = pg_dist_node.nodeport
)
SELECT
 waiting.pid AS waiting_pid,
 blocking.pid AS blocking_pid,
 waiting.query AS blocked_statement,
 blocking.query AS current_statement_in_blocking_process,
 waiting.initiator_node_id AS waiting_node_id,
 blocking.initiator_node_id AS blocking_node_id

FROM
 dump_global_wait_edges() as global_wait_edges
JOIN
 citus_dist_stat_activity_with_node_id waiting ON (global_wait_edges.waiting_transaction_num = waiting.transaction_number AND global_wait_edges.waiting_node_id = waiting.initiator_node_id)
JOIN
 citus_dist_stat_activity_with_node_id blocking ON (global_wait_edges.blocking_transaction_num = blocking.transaction_number AND global_wait_edges.blocking_node_id = blocking.initiator_node_id);

ALTER VIEW citus.citus_lock_waits SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_lock_waits TO PUBLIC;

RESET search_path;
