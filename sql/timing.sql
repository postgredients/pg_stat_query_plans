-- Execution time is reported in milliseconds in both statistics views.
SET pg_stat_query_plans.track = 'top';
SET pg_stat_query_plans.track_utility = FALSE;
SELECT pg_stat_query_plans_reset() IS NOT NULL AS t;

-- Bracket the statement with wall-clock timestamps instead of imposing a
-- fixed upper time limit on slow machines. The sleep makes a unit error
-- visible even for a statement that otherwise does very little work.
SELECT clock_timestamp() AS started \gset
SELECT pg_sleep(0.01);
SELECT 1000 * extract(epoch FROM clock_timestamp() - :'started'::timestamptz)
  AS elapsed_ms \gset

SELECT calls, total_exec_time > 0 AND total_exec_time <= :elapsed_ms
  AS milliseconds
FROM pg_stat_query_plans_sql
WHERE query LIKE 'SELECT pg_sleep%';

SELECT calls, total_exec_time > 0 AND total_exec_time <= :elapsed_ms
  AS milliseconds
FROM pg_stat_query_plans
WHERE query LIKE 'SELECT pg_sleep%';
