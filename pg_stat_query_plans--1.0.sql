/* contrib/pg_stat_query_plans/pg_stat_query_plans--1.0.sql */

--- Define reset statistics
CREATE FUNCTION pg_stat_query_plans_reset(IN userid Oid DEFAULT 0,
	IN dbid Oid DEFAULT 0,
	IN queryid bigint DEFAULT 0
)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_stat_query_plans_reset_queryid'
LANGUAGE C STRICT PARALLEL SAFE;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION pg_stat_query_plans_reset(Oid, Oid, bigint) FROM PUBLIC;

---- Define reset minmax values in statistics
CREATE FUNCTION pg_stat_query_plans_reset_minmax()
RETURNS void
AS 'MODULE_PATHNAME', 'pg_stat_query_plans_reset_minmax'
LANGUAGE C STRICT PARALLEL SAFE;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION pg_stat_query_plans_reset_minmax() FROM PUBLIC;


--- Define pg_stat_query_plans_info
CREATE FUNCTION pg_stat_query_plans_info(
	OUT dealloc bigint,
	OUT stats_reset timestamp with time zone,
	OUT gc_count bigint,
	OUT generation bigint,
	OUT storage_offset bigint,
	OUT queries_text_size bigint,
	OUT queries_compressed_text_size bigint,
	OUT plans_text_size bigint,
	OUT plans_compressed_text_size bigint,
	OUT queries bigint,
	OUT execution_plans bigint,
	OUT queries_wiped_out bigint,
	OUT plans_wiped_out bigint,
	OUT dealloc_time_ms bigint,
	OUT gc_time_ms bigint
)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE VIEW pg_stat_query_plans_info AS
  SELECT * FROM pg_stat_query_plans_info();

GRANT SELECT ON pg_stat_query_plans_info TO PUBLIC;

/* Now redefine */
CREATE FUNCTION pg_stat_query_plans_sql(IN showtext boolean,
	OUT userid oid,
	OUT dbid oid,
	OUT toplevel bool,
	OUT queryid bigint,
	OUT query text,
	OUT plans int8,
	OUT total_plan_time float8,
	OUT min_plan_time float8,
	OUT max_plan_time float8,
	OUT mean_plan_time float8,
	OUT stddev_plan_time float8,
	OUT calls int8,
	OUT total_exec_time float8,
	OUT min_exec_time float8,
	OUT max_exec_time float8,
	OUT mean_exec_time float8,
	OUT stddev_exec_time float8,
	OUT rows int8,
	OUT shared_blks_hit int8,
	OUT shared_blks_read int8,
	OUT shared_blks_dirtied int8,
	OUT shared_blks_written int8,
	OUT local_blks_hit int8,
	OUT local_blks_read int8,
	OUT local_blks_dirtied int8,
	OUT local_blks_written int8,
	OUT temp_blks_read int8,
	OUT temp_blks_written int8,
	OUT blk_read_time float8,
	OUT blk_write_time float8,
	OUT temp_blk_read_time float8,
	OUT temp_blk_write_time float8,
	OUT wal_records int8,
	OUT wal_fpi int8,
	OUT wal_bytes numeric,
	OUT jit_functions int8,
	OUT jit_generation_time float8,
	OUT jit_inlining_count int8,
	OUT jit_inlining_time float8,
	OUT jit_optimization_count int8,
	OUT jit_optimization_time float8,
	OUT jit_emission_count int8,
	OUT jit_emission_time float8,
	OUT generation bigint
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stat_query_plans_sql_1_0'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE VIEW pg_stat_query_plans_sql AS
  SELECT * FROM pg_stat_query_plans_sql(true);

GRANT SELECT ON pg_stat_query_plans_sql TO PUBLIC;

CREATE FUNCTION pg_stat_query_plans(IN showtext boolean,
	OUT userid oid,
	OUT dbid oid,
	OUT toplevel bool,
	OUT queryid bigint,
	OUT planid bigint,
	OUT query text,
	OUT normalized_plan text,
	OUT example_plan text,
	OUT calls int8,
	OUT total_exec_time float8,
	OUT min_exec_time float8,
	OUT max_exec_time float8,
	OUT mean_exec_time float8,
	OUT stddev_exec_time float8,
	OUT rows int8,
	OUT shared_blks_hit int8,
	OUT shared_blks_read int8,
	OUT shared_blks_dirtied int8,
	OUT shared_blks_written int8,
	OUT local_blks_hit int8,
	OUT local_blks_read int8,
	OUT local_blks_dirtied int8,
	OUT local_blks_written int8,
	OUT temp_blks_read int8,
	OUT temp_blks_written int8,
	OUT blk_read_time float8,
	OUT blk_write_time float8,
	OUT temp_blk_read_time float8,
	OUT temp_blk_write_time float8,
	OUT wal_records int8,
	OUT wal_fpi int8,
	OUT wal_bytes numeric,
	OUT jit_functions int8,
	OUT jit_generation_time float8,
	OUT jit_inlining_count int8,
	OUT jit_inlining_time float8,
	OUT jit_optimization_count int8,
	OUT jit_optimization_time float8,
	OUT jit_emission_count int8,
	OUT jit_emission_time float8,
	OUT startup_cost float8,
	OUT total_cost float8,
	OUT plan_rows bigint,
	OUT plan_width bigint,
	OUT generation bigint
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stat_query_plans_plans_1_0'
LANGUAGE C STRICT VOLATILE PARALLEL SAFE;

CREATE VIEW pg_stat_query_plans AS
  SELECT * FROM pg_stat_query_plans(true);

GRANT SELECT ON pg_stat_query_plans TO PUBLIC;
