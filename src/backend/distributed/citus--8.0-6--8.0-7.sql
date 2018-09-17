/* citus--8.0-6--8.0-7 */
SET search_path = 'pg_catalog';

CREATE VIEW citus.citus_lock_waits AS

WITH citus_dist_stat_activity AS
(
  SELECT * FROM citus_dist_stat_activity
)
SELECT
  waiting.pid AS waiting_pid,
  blocking.pid AS blocking_pid,
  waiting.query AS blocked_statement,
  blocking.query AS current_statement_in_blocking_process
FROM
  dump_global_wait_edges() as global_wait_edges
JOIN
  citus_dist_stat_activity waiting ON (global_wait_edges.waiting_transaction_num = waiting.transaction_number)
JOIN
  citus_dist_stat_activity blocking ON (global_wait_edges.blocking_transaction_num = blocking.transaction_number);

ALTER VIEW citus.citus_lock_waits SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_lock_waits TO PUBLIC;

RESET search_path;
