-- SQL evaluated by constant folding is nested, just like SQL executed
-- from a function at execution time.
SET pg_stat_query_plans.track_utility = FALSE;
SET pg_stat_query_plans.track_planning = TRUE;
SET pg_stat_query_plans.track = 'all';
CREATE FUNCTION pgqp_folded(i integer) RETURNS integer
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE v integer;
BEGIN
  SELECT g + 1 AS pgqp_folded_value INTO v FROM generate_series(i, i) AS g;
  RETURN v;
END
$$;
SELECT pg_stat_query_plans_reset() IS NOT NULL AS t;

SELECT pgqp_folded(41);
SELECT toplevel, plans, calls FROM pg_stat_query_plans_sql
WHERE query LIKE 'SELECT g + %AS pgqp_folded_value%';
SELECT toplevel, calls FROM pg_stat_query_plans
WHERE query LIKE 'SELECT g + %AS pgqp_folded_value%';

-- Reuse the function's cached plan with planning statistics disabled.
SET pg_stat_query_plans.track_planning = FALSE;
SELECT pg_stat_query_plans_reset() IS NOT NULL AS t;
SELECT pgqp_folded(42);
SELECT toplevel, plans, calls FROM pg_stat_query_plans_sql
WHERE query LIKE 'SELECT g + %AS pgqp_folded_value%';

-- Neither mode of planning accounting should expose the inner query
-- with top-level-only tracking. The outer query must still be counted.
SET pg_stat_query_plans.track = 'top';
SET pg_stat_query_plans.track_planning = TRUE;
SELECT pg_stat_query_plans_reset() IS NOT NULL AS t;
SELECT pgqp_folded(43);
SELECT count(*) AS inner_queries FROM pg_stat_query_plans_sql
WHERE query LIKE 'SELECT g + %AS pgqp_folded_value%';
SELECT toplevel, plans, calls FROM pg_stat_query_plans_sql
WHERE query LIKE 'SELECT pgqp_folded%';

SET pg_stat_query_plans.track_planning = FALSE;
SELECT pg_stat_query_plans_reset() IS NOT NULL AS t;
SELECT pgqp_folded(44);
SELECT count(*) AS inner_queries FROM pg_stat_query_plans_sql
WHERE query LIKE 'SELECT g + %AS pgqp_folded_value%';
SELECT toplevel, plans, calls FROM pg_stat_query_plans_sql
WHERE query LIKE 'SELECT pgqp_folded%';

-- A planning error must restore nesting depth in both planner paths.
SELECT 1 / 0;
SELECT pgqp_folded(45);
SET pg_stat_query_plans.track_planning = TRUE;
SELECT 1 / 0;
SELECT pgqp_folded(46);
SELECT toplevel, plans, calls FROM pg_stat_query_plans_sql
WHERE query LIKE 'SELECT pgqp_folded%';

DROP FUNCTION pgqp_folded(integer);
RESET pg_stat_query_plans.track;
RESET pg_stat_query_plans.track_planning;
RESET pg_stat_query_plans.track_utility;
