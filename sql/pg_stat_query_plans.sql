CREATE EXTENSION pg_stat_query_plans;

--
-- simple and compound statements
--
SET pg_stat_query_plans.track_utility = FALSE;
SET pg_stat_query_plans.track_planning = TRUE;
SELECT pg_stat_query_plans_reset();

SELECT 1 AS "int";

SELECT 'hello'
  -- multiline
  AS "text";

SELECT 'world' AS "text";

-- transaction
BEGIN;
SELECT 1 AS "int";
SELECT 'hello' AS "text";
COMMIT;

-- compound transaction
BEGIN \;
SELECT 2.0 AS "float" \;
SELECT 'world' AS "text" \;
COMMIT;

-- compound with empty statements and spurious leading spacing
\;\;   SELECT 3 + 3 \;\;\;   SELECT ' ' || ' !' \;\;   SELECT 1 + 4 \;;

-- non ;-terminated statements
SELECT 1 + 1 + 1 AS "add" \gset
SELECT :add + 1 + 1 AS "add" \;
SELECT :add + 1 + 1 AS "add" \gset

-- set operator
SELECT 1 AS i UNION SELECT 2 ORDER BY i;

-- ? operator
select '{"a":1, "b":2}'::jsonb ? 'b';

-- cte
WITH t(f) AS (
  VALUES (1.0), (2.0)
)
  SELECT f FROM t ORDER BY f;

-- prepared statement with parameter
PREPARE pgqp_test (int) AS SELECT $1, 'test' LIMIT 1;
EXECUTE pgqp_test(1);
DEALLOCATE pgqp_test;

SELECT query, normalized_plan, calls, rows FROM pg_stat_query_plans ORDER BY query COLLATE "C";

--
-- CRUD: INSERT SELECT UPDATE DELETE on test table
--

SELECT pg_stat_query_plans_reset();

-- utility "create table" should not be shown
CREATE TEMP TABLE test (a int, b char(20));

INSERT INTO test VALUES(generate_series(1, 10), 'aaa');
UPDATE test SET b = 'bbb' WHERE a > 7;
DELETE FROM test WHERE a > 9;

-- explicit transaction
BEGIN;
UPDATE test SET b = '111' WHERE a = 1 ;
COMMIT;

BEGIN \;
UPDATE test SET b = '222' WHERE a = 2 \;
COMMIT ;

UPDATE test SET b = '333' WHERE a = 3 \;
UPDATE test SET b = '444' WHERE a = 4 ;

BEGIN \;
UPDATE test SET b = '555' WHERE a = 5 \;
UPDATE test SET b = '666' WHERE a = 6 \;
COMMIT ;

-- many INSERT values
INSERT INTO test (a, b) VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- SELECT with constants
SELECT * FROM test WHERE a > 5 ORDER BY a ;

SELECT *
  FROM test
  WHERE a > 9
  ORDER BY a ;

-- SELECT without constants
SELECT * FROM test ORDER BY a;

-- SELECT with IN clause
SELECT * FROM test WHERE a IN (1, 2, 3, 4, 5);

SELECT query, normalized_plan, calls, rows FROM pg_stat_query_plans ORDER BY query COLLATE "C";

--
-- pg_stat_query_plans.track = none
--
SET pg_stat_query_plans.track = 'none';
SELECT pg_stat_query_plans_reset();

SELECT 1 AS "one";
SELECT 1 + 1 AS "two";

SELECT query, normalized_plan, calls, rows FROM pg_stat_query_plans ORDER BY query COLLATE "C";

--
-- pg_stat_query_plans.track = top
--
SET pg_stat_query_plans.track = 'top';
SELECT pg_stat_query_plans_reset();

DO LANGUAGE plpgsql $$
BEGIN
  -- this is a SELECT
  PERFORM 'hello world'::TEXT;
END;
$$;

-- PL/pgSQL function
CREATE FUNCTION PLUS_TWO(i INTEGER) RETURNS INTEGER AS $$
DECLARE
  r INTEGER;
BEGIN
  SELECT (i + 1 + 1.0)::INTEGER INTO r;
  RETURN r;
END; $$ LANGUAGE plpgsql;

SELECT PLUS_TWO(3);
SELECT PLUS_TWO(7);

-- SQL function --- use LIMIT to keep it from being inlined
CREATE FUNCTION PLUS_ONE(i INTEGER) RETURNS INTEGER AS
$$ SELECT (i + 1.0)::INTEGER LIMIT 1 $$ LANGUAGE SQL;

SELECT PLUS_ONE(8);
SELECT PLUS_ONE(10);

SELECT query, normalized_plan, calls, rows FROM pg_stat_query_plans ORDER BY query COLLATE "C";

--
-- pg_stat_query_plans.track = all
--
SET pg_stat_query_plans.track = 'all';
SELECT pg_stat_query_plans_reset();

-- we drop and recreate the functions to avoid any caching funnies
DROP FUNCTION PLUS_ONE(INTEGER);
DROP FUNCTION PLUS_TWO(INTEGER);

-- PL/pgSQL function
CREATE FUNCTION PLUS_TWO(i INTEGER) RETURNS INTEGER AS $$
DECLARE
  r INTEGER;
BEGIN
  SELECT (i + 1 + 1.0)::INTEGER INTO r;
  RETURN r;
END; $$ LANGUAGE plpgsql;

SELECT PLUS_TWO(-1);
SELECT PLUS_TWO(2);

-- SQL function --- use LIMIT to keep it from being inlined
CREATE FUNCTION PLUS_ONE(i INTEGER) RETURNS INTEGER AS
$$ SELECT (i + 1.0)::INTEGER LIMIT 1 $$ LANGUAGE SQL;

SELECT PLUS_ONE(3);
SELECT PLUS_ONE(1);

SELECT query, normalized_plan, calls, rows FROM pg_stat_query_plans ORDER BY query COLLATE "C";

--
-- queries with locking clauses
--
CREATE TABLE pgqp_a (id integer PRIMARY KEY);
CREATE TABLE pgqp_b (id integer PRIMARY KEY, a_id integer REFERENCES pgqp_a);

SELECT pg_stat_query_plans_reset();

-- control query
SELECT * FROM pgqp_a JOIN pgqp_b ON pgqp_b.a_id = pgqp_a.id;

-- test range tables
SELECT * FROM pgqp_a JOIN pgqp_b ON pgqp_b.a_id = pgqp_a.id FOR UPDATE;
SELECT * FROM pgqp_a JOIN pgqp_b ON pgqp_b.a_id = pgqp_a.id FOR UPDATE OF pgqp_a;
SELECT * FROM pgqp_a JOIN pgqp_b ON pgqp_b.a_id = pgqp_a.id FOR UPDATE OF pgqp_b;
SELECT * FROM pgqp_a JOIN pgqp_b ON pgqp_b.a_id = pgqp_a.id FOR UPDATE OF pgqp_a, pgqp_b; -- matches plain "FOR UPDATE"
SELECT * FROM pgqp_a JOIN pgqp_b ON pgqp_b.a_id = pgqp_a.id FOR UPDATE OF pgqp_b, pgqp_a;

-- test strengths
SELECT * FROM pgqp_a JOIN pgqp_b ON pgqp_b.a_id = pgqp_a.id FOR NO KEY UPDATE;
SELECT * FROM pgqp_a JOIN pgqp_b ON pgqp_b.a_id = pgqp_a.id FOR SHARE;
SELECT * FROM pgqp_a JOIN pgqp_b ON pgqp_b.a_id = pgqp_a.id FOR KEY SHARE;

-- test wait policies
SELECT * FROM pgqp_a JOIN pgqp_b ON pgqp_b.a_id = pgqp_a.id FOR UPDATE NOWAIT;
SELECT * FROM pgqp_a JOIN pgqp_b ON pgqp_b.a_id = pgqp_a.id FOR UPDATE SKIP LOCKED;

SELECT calls, planid, normalized_plan, query FROM pg_stat_query_plans ORDER BY query COLLATE "C";

DROP TABLE pgqp_a, pgqp_b CASCADE;

--
-- utility commands
--
SET pg_stat_query_plans.track_utility = TRUE;
SELECT pg_stat_query_plans_reset();

SELECT 1;
-- all functions below do not have plan so must not be shown in plans view
CREATE INDEX test_b ON test(b);
DROP TABLE test \;
DROP TABLE IF EXISTS test \;
DROP FUNCTION PLUS_ONE(INTEGER);
DROP TABLE IF EXISTS test \;
DROP TABLE IF EXISTS test \;
DROP FUNCTION IF EXISTS PLUS_ONE(INTEGER);
DROP FUNCTION PLUS_TWO(INTEGER);

SELECT query, normalized_plan, calls, rows FROM pg_stat_query_plans ORDER BY query COLLATE "C";

--
-- Track user activity and reset them
--
SELECT pg_stat_query_plans_reset();
CREATE ROLE regress_stats_user1;
CREATE ROLE regress_stats_user2;

SET ROLE regress_stats_user1;

SELECT 1 AS "ONE";
SELECT 1+1 AS "TWO";

RESET ROLE;
SET ROLE regress_stats_user2;

SELECT 1 AS "ONE";
SELECT 1+1 AS "TWO";

RESET ROLE;
SELECT query, normalized_plan, calls, rows FROM pg_stat_query_plans ORDER BY query COLLATE "C";

--
-- Don't reset anything if any of the parameter is NULL
--
SELECT pg_stat_query_plans_reset(NULL);
SELECT query, normalized_plan, calls, rows FROM pg_stat_query_plans ORDER BY query COLLATE "C";

--
-- remove query ('SELECT $1+$2 AS "TWO"') executed by regress_stats_user2
-- in the current_database
--
SELECT pg_stat_query_plans_reset(
	(SELECT r.oid FROM pg_roles AS r WHERE r.rolname = 'regress_stats_user2'),
	(SELECT d.oid FROM pg_database As d where datname = current_database()),
	(SELECT s.queryid FROM pg_stat_query_plans_sql AS s
				WHERE s.query = 'SELECT $1+$2 AS "TWO"' LIMIT 1));
SELECT query, normalized_plan, calls, rows FROM pg_stat_query_plans ORDER BY query COLLATE "C";

--
-- remove query ('SELECT $1 AS "ONE"') executed by two users
--
SELECT pg_stat_query_plans_reset(0,0,s.queryid)
	FROM pg_stat_query_plans_sql AS s WHERE s.query = 'SELECT $1 AS "ONE"';
SELECT query, normalized_plan, calls, rows FROM pg_stat_query_plans ORDER BY query COLLATE "C";

--
-- remove query of a user (regress_stats_user1)
--
SELECT pg_stat_query_plans_reset(r.oid)
		FROM pg_roles AS r WHERE r.rolname = 'regress_stats_user1';
SELECT query, normalized_plan, calls, rows FROM pg_stat_query_plans ORDER BY query COLLATE "C";

--
-- reset all
--
SELECT pg_stat_query_plans_reset(0,0,0);
SELECT query, normalized_plan, calls, rows FROM pg_stat_query_plans ORDER BY query COLLATE "C";

--
-- cleanup
--
DROP ROLE regress_stats_user1;
DROP ROLE regress_stats_user2;

--
-- top level handling
--
SELECT pg_stat_query_plans_reset();
CREATE TABLE test ();
ALTER TABLE test ADD COLUMN x int;

SET pg_stat_query_plans.track = 'top';
DELETE FROM test;
DO $$
BEGIN
    DELETE FROM test;
END;
$$ LANGUAGE plpgsql;

SELECT query, normalized_plan, toplevel, calls FROM
pg_stat_query_plans WHERE query LIKE '%DELETE%' ORDER BY query COLLATE "C", toplevel;

SET pg_stat_query_plans.track = 'all';
DELETE FROM test;
DO $$
BEGIN
    DELETE FROM test;
END;
$$ LANGUAGE plpgsql;

SELECT query, normalized_plan, toplevel, calls FROM
pg_stat_query_plans WHERE query LIKE '%DELETE%' ORDER BY query COLLATE "C", toplevel;

DROP TABLE IF EXISTS test \;

SELECT pg_stat_query_plans_reset();

-- FROM [ONLY]
CREATE TABLE tbl_inh(id integer);
CREATE TABLE tbl_inh_1() INHERITS (tbl_inh);
INSERT INTO tbl_inh_1 SELECT 1;

SELECT * FROM tbl_inh;
SELECT * FROM ONLY tbl_inh;

SELECT COUNT(*) FROM pg_stat_query_plans WHERE query LIKE '%FROM%tbl_inh%';

DROP TABLE tbl_inh CASCADE;

-- WITH TIES
CREATE TABLE limitoption AS SELECT 0 AS val FROM generate_series(1, 10);
SELECT *
FROM limitoption
WHERE val < 2
ORDER BY val
FETCH FIRST 2 ROWS WITH TIES;

SELECT *
FROM limitoption
WHERE val < 2
ORDER BY val
FETCH FIRST 2 ROW ONLY;

SELECT COUNT(*) FROM pg_stat_query_plans WHERE query LIKE '%FETCH FIRST%';

DROP TABLE limitoption;

-- GROUP BY [DISTINCT]
SELECT a, b, c
FROM (VALUES (1, 2, 3), (4, NULL, 6), (7, 8, 9)) AS t (a, b, c)
GROUP BY ROLLUP(a, b), rollup(a, c)
ORDER BY a, b, c;
SELECT a, b, c
FROM (VALUES (1, 2, 3), (4, NULL, 6), (7, 8, 9)) AS t (a, b, c)
GROUP BY DISTINCT ROLLUP(a, b), rollup(a, c)
ORDER BY a, b, c;

SELECT COUNT(*) FROM pg_stat_query_plans WHERE query LIKE '%GROUP BY%ROLLUP%';

-- GROUPING SET agglevelsup
SELECT (
  SELECT (
    SELECT GROUPING(a,b) FROM (VALUES (1)) v2(c)
  ) FROM (VALUES (1,2)) v1(a,b) GROUP BY (a,b)
) FROM (VALUES(6,7)) v3(e,f) GROUP BY ROLLUP(e,f);
SELECT (
  SELECT (
    SELECT GROUPING(e,f) FROM (VALUES (1)) v2(c)
  ) FROM (VALUES (1,2)) v1(a,b) GROUP BY (a,b)
) FROM (VALUES(6,7)) v3(e,f) GROUP BY ROLLUP(e,f);

SELECT COUNT(*) FROM pg_stat_query_plans WHERE query LIKE '%SELECT GROUPING%';

DROP EXTENSION pg_stat_query_plans;
