-- Test statistics collected after a min/max reset.

SET pg_stat_query_plans.track_utility = FALSE;
SET pg_stat_query_plans.track_planning = TRUE;
SELECT pg_stat_query_plans_reset() IS NOT NULL AS t;

SELECT 1 AS minmax_reset_test;
SELECT 1 AS minmax_reset_test;

SELECT pg_stat_query_plans_reset_minmax() IS NOT NULL AS t;

-- Cumulative totals should survive the reset, while the distribution
-- statistics should describe only executions after the reset.
SELECT 1 AS minmax_reset_test;

SELECT plans > 1 AS cumulative_plans,
       min_plan_time = max_plan_time AND
         max_plan_time = mean_plan_time AS one_plan_sample,
       stddev_plan_time = 0 AS zero_plan_stddev,
       calls > 1 AS cumulative_calls,
       min_exec_time = max_exec_time AND
         max_exec_time = mean_exec_time AS one_exec_sample,
       stddev_exec_time = 0 AS zero_exec_stddev
FROM pg_stat_query_plans_sql
WHERE query = 'SELECT $1 AS minmax_reset_test';

SELECT calls > 1 AS cumulative_calls,
       min_exec_time = max_exec_time AND
         max_exec_time = mean_exec_time AS one_exec_sample,
       stddev_exec_time = 0 AS zero_exec_stddev
FROM pg_stat_query_plans
WHERE query = 'SELECT 1 AS minmax_reset_test;';
