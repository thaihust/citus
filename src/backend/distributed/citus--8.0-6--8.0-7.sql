/* citus--8.0-6--8.0-7 */
SET search_path = 'pg_catalog';

CREATE VIEW citus.citus_lock_waits AS

WITH get_global_active_transactions_cache AS
(
  SELECT * FROM get_global_active_transactions()
),
citus_dist_stat_activity_common AS
(
  SELECT * FROM citus_dist_stat_activity
),
citus_wait_pids AS (
  SELECT ggat.process_id as waiting_pid, ggat_2.process_id as blocking_pid FROM dump_global_wait_edges() as dgwe JOIN get_global_active_transactions_cache as ggat ON (dgwe.waiting_node_id = ggat.initiator_node_identifier AND dgwe.waiting_transaction_num = ggat.transaction_number AND ggat.worker_query = false) JOIN get_global_active_transactions_cache as ggat_2 ON (dgwe.blocking_node_id = ggat_2.initiator_node_identifier AND dgwe.blocking_transaction_num = ggat_2.transaction_number AND ggat_2.worker_query = false)
)
SELECT
  waiting_pid AS blocked_pid,
  blocking_pid,
  waiting.query AS blocked_statement,
  blocking.query AS current_statement_in_blocking_process
FROM
  citus_wait_pids
JOIN
  citus_dist_stat_activity_common waiting ON (waiting_pid = waiting.pid)
JOIN
  citus_dist_stat_activity_common blocking ON (blocking_pid = blocking.pid);

ALTER VIEW citus.citus_lock_waits SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_lock_waits TO PUBLIC;

RESET search_path;
