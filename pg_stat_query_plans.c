/*-------------------------------------------------------------------------
 *
 * pg_stat_query_plans.c
 *		Track statement planning and execution times as well as resource
 *		usage across a whole database cluster.
 *
 * Execution costs are totaled for each distinct source query, and kept in
 * a shared hashtable.  (We track only as many distinct queries as will fit
 * in the designated amount of shared memory.)
 *
 * Like original pg_stat_query_plans this module normalized query entries.  As of
 * Postgres 14, the normalization is done by the core if compute_query_id is
 * enabled, or optionally by third-party modules.
 *
 * To facilitate presenting entries to users, we create "representative" query
 * strings in which constants are replaced with parameter symbols ($n), to
 * make it clearer what a normalized entry can represent.  To save on shared
 * memory, and to avoid having to truncate oversized query strings, all texts
 * are comoressed.
 *
 * Note about locking issues: to create or delete an entry in the shared
 * hashtable, one must hold pgqp->lock exclusively.  Modifying any field
 * in an entry except the counters requires the same.  To look up an entry,
 * one must hold the lock shared.  To read or update the counters within
 * an entry, one must hold the lock shared or exclusive (so the entry doesn't
 * disappear!) and also take the entry's mutex spinlock.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/parallel.h"
#include "catalog/pg_authid.h"
#if PG_VERSION_NUM >= 130000
#include "common/hashfn.h"
#elif PG_VERSION_NUM >= 120000
#include "utils/hashutils.h"
#else
#include "access/hash.h"
#endif
#include "commands/explain.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "jit/jit.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/scanner.h"
#include "parser/scansup.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#if PG_VERSION_NUM >= 140000 && PG_VERSION_NUM < 160000
#include "utils/queryjumble.h"
#endif
#if PG_VERSION_NUM >= 160000
#include "nodes/queryjumble.h"
#endif
#if PG_VERSION_NUM < 120000
#include "access/htup_details.h"
#endif
#include "pg_stat_query_plans_common.h"
#include "pg_stat_query_plans_parser.h"
#include "pg_stat_query_plans_storage.h"
#include "utils/errcodes.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_stat_query_plans_reset_queryid);
PG_FUNCTION_INFO_V1(pg_stat_query_plans_reset_minmax);
PG_FUNCTION_INFO_V1(pg_stat_query_plans_sql_1_0);
PG_FUNCTION_INFO_V1(pg_stat_query_plans_plans_1_0);
PG_FUNCTION_INFO_V1(pg_stat_query_plans_info);

/*---- Global variables ----*/

/* Current nesting depth of ExecutorRun+ProcessUtility calls */
int exec_nested_level = 0;

/* Current nesting depth of planner calls */
int plan_nested_level = 0;

const struct config_enum_entry track_options[] = {
    {"none", PGQP_TRACK_NONE, false},
    {"top", PGQP_TRACK_TOP, false},
    {"all", PGQP_TRACK_ALL, false},
    {NULL, 0, false}};

const struct config_enum_entry plan_formats[] = {
    {"text", EXPLAIN_FORMAT_TEXT, false},
    {"json", EXPLAIN_FORMAT_JSON, false},
    {"yaml", EXPLAIN_FORMAT_YAML, false},
    {"xml", EXPLAIN_FORMAT_XML, false},
    {NULL, 0, false}};

const struct config_enum_entry text_encodings[] = {
    {"plaintext", PGQP_PLAINTEXT, false},
    {"pglz", PGQP_PGLZ, false},
    {NULL, 0, false}};

/*---- Local vriables ----*/

/* Saved hook values in case of unload */
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/* Links to shared memory state */
pgqpSharedState *pgqp = NULL;
HTAB *pgqp_queries = NULL;
HTAB *pgqp_plans = NULL;
HTAB *pgqp_texts = NULL;
char *pgqp_storage = NULL;

const int MEAN_PLANS_PER_QUERY = 2;

int pgqp_max;              /* max # statements to track */
int pgqp_max_plans;        /* max # plans to track */
int pgqp_storage_memory;   /* memory used to store plan and query texts */
int pgqp_max_query_len;    /* maximum query text length */
int pgqp_max_plan_len;     /* maximum execution plan text length */
int pgqp_track;            /* tracking level */
int pgqp_encoding;         /* compress stored texts */
bool pgqp_track_utility;   /* whether to track utility commands */
bool pgqp_track_planning;  /* whether to track planning duration */
bool pgqp_track_plans;     /* track execution plans or not */
bool pgqp_normalize_plans; /* Normalize plans so pans differ in constans have
                              the same planId */
int example_plan_format;   /* Plan representation style */
bool example_log_verbose;  /* Set VERBOSE for EXPLAIN on logging */
bool example_log_triggers; /* Log trigger trace in EXPLAIN */

/*---- Function declarations ----*/

void _PG_init(void);
void _PG_fini(void);

#if PG_VERSION_NUM >= 150000
static void pgqp_shmem_request(void);
#endif
static void pgqp_shmem_startup(void);
#if PG_VERSION_NUM >= 140000
static void pgqp_post_parse_analyze(ParseState *pstate, Query *query,
                                    JumbleState *jstate);
#else
static void pgqp_post_parse_analyze(ParseState *pstate, Query *query);
#endif
#if PG_VERSION_NUM >= 130000
static PlannedStmt *pgqp_planner(Query *parse, const char *query_string,
                                 int cursorOptions, ParamListInfo boundParams);
#endif
static void pgqp_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgqp_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
                             uint64 count, bool execute_once);
static void pgqp_ExecutorFinish(QueryDesc *queryDesc);
static void pgqp_ExecutorEnd(QueryDesc *queryDesc);
#if PG_VERSION_NUM >= 140000
static void pgqp_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                                bool readOnlyTree,
                                ProcessUtilityContext context,
                                ParamListInfo params,
                                QueryEnvironment *queryEnv, DestReceiver *dest,
                                QueryCompletion *qc);
#else
static void pgqp_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                                ProcessUtilityContext context,
                                ParamListInfo params,
                                QueryEnvironment *queryEnv, DestReceiver *dest,
#if PG_VERSION_NUM >= 130000
                                QueryCompletion *qc
#else
                                char *qc
#endif
);
#endif

static int pgqp_add_counters_data(Counters *c, Datum values[0],
                                  pgqpStoreKind start_kind);

static void pg_stat_query_plans_stat_internal(FunctionCallInfo fcinfo,
                                                pgqpVersion api_version,
                                                bool showtext);

static void pg_stat_query_plans_plan_internal(FunctionCallInfo fcinfo,
                                                pgqpVersion api_version,
                                                bool showtext);
static Size pgqp_memsize(void);

/*
 * Module load callback
 */
void _PG_init(void) {
  /*
   * In order to create our shared memory area, we have to be loaded via
   * shared_preload_libraries.  If not, fall out without hooking into any of
   * the main system.  (We don't throw error here because it seems useful to
   * allow the pg_stat_query_plans functions to be created even when the
   * module isn't active.  The functions must protect themselves against
   * being called then, however.)
   */
  if (!process_shared_preload_libraries_in_progress)
    return;

/*
 * Inform the postmaster that we want to enable query_id calculation if
 * compute_query_id is set to auto.
 */
#if PG_VERSION_NUM >= 140000
  EnableQueryId();
#endif

  /*
   * Define (or redefine) custom GUC variables.
   */
  DefineCustomIntVariable(
      "pg_stat_query_plans.max",
      "Sets the maximum number of statements tracked by pg_stat_query_plans.",
      NULL, &pgqp_max, 10000, 100, INT_MAX / 2, PGC_POSTMASTER, 0, NULL, NULL,
      NULL);

  DefineCustomIntVariable(
      "pg_stat_query_plans.max_query_len",
      "Sets the maximum length of query texts stored by pg_stat_query_plans.",
      NULL, &pgqp_max_query_len, 4 * 1024 * 1024, 100, 6 * 1024 * 1024,
      PGC_SUSET, 0, NULL, NULL, NULL);

  DefineCustomIntVariable("pg_stat_query_plans.max_plan_size",
                          "Sets the maximum length of execution plan texts "
                          "stored by pg_stat_query_plans.",
                          NULL, &pgqp_max_plan_len, 6 * 1024 * 1024, 1024,
                          8 * 1024 * 1024, PGC_SUSET, 0, NULL, NULL, NULL);

  DefineCustomIntVariable(
      "pg_stat_query_plans.storage_memory",
      "Sets the amount of memory used to store query and plan texts", NULL,
      &pgqp_storage_memory, 256 * 1024 * 1024, 16 * 1024 * 1024, INT_MAX,
      PGC_POSTMASTER, 0, NULL, NULL, NULL);

  DefineCustomEnumVariable(
      "pg_stat_query_plans.track",
      "Selects which statements are tracked by pg_stat_query_plans.", NULL,
      &pgqp_track, PGQP_TRACK_ALL, track_options, PGC_SUSET, 0, NULL, NULL,
      NULL);

  DefineCustomBoolVariable(
      "pg_stat_query_plans.track_utility",
      "Selects whether utility commands are tracked by pg_stat_query_plans.",
      NULL, &pgqp_track_utility, true, PGC_SUSET, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable(
      "pg_stat_query_plans.track_planning",
      "Selects whether planning duration is tracked by pg_stat_query_plans.",
      NULL, &pgqp_track_planning, false, PGC_SUSET, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable("pg_stat_query_plans.track_plans",
                           "Selects whether execution plans (texts and "
                           "statistics) is tracked by pg_stat_query_plans.",
                           NULL, &pgqp_track_plans, true, PGC_SUSET, 0, NULL,
                           NULL, NULL);

  DefineCustomBoolVariable("pg_stat_query_plans.normalize_plans",
                           "Normalize plans - remove  constants and unstable "
                           "values to generate the same planId.",
                           NULL, &pgqp_normalize_plans, true, PGC_SUSET, 0,
                           NULL, NULL, NULL);

  DefineCustomEnumVariable("pg_stat_query_plans.pgqp_encoding",
                           "Set compression method for stored texts", NULL,
                           &pgqp_encoding, PGQP_PGLZ, text_encodings, PGC_SUSET,
                           0, NULL, NULL, NULL);

  DefineCustomEnumVariable("pg_stat_query_plans.example_plan_format",
                           "Selects which format to be appied for plan "
                           "representation in pg_stat_query_plans.",
                           NULL, &example_plan_format, EXPLAIN_FORMAT_JSON,
                           plan_formats, PGC_SUSET, 0, NULL, NULL, NULL);

  DefineCustomBoolVariable("pg_stat_query_plans.example_plan_verbose",
                           "Set VERBOSE for EXPLAIN on logging plan.", NULL,
                           &example_log_verbose, true, PGC_SUSET, 0, NULL, NULL,
                           NULL);

  DefineCustomBoolVariable("pg_stat_query_plans.example_plan_triggers",
                           "Log trigger trace in EXPLAIN.", NULL,
                           &example_log_triggers, true, PGC_SUSET, 0, NULL,
                           NULL, NULL);

#if PG_VERSION_NUM >= 150000
  MarkGUCPrefixReserved("pg_stat_query_plans");
#else
  EmitWarningsOnPlaceholders("pg_stat_query_plans");
#endif

/*
 * Install hooks.
 */
#if PG_VERSION_NUM >= 150000
  prev_shmem_request_hook = shmem_request_hook;
  shmem_request_hook = pgqp_shmem_request;
#else
  /* Reserve memory there */
  RequestAddinShmemSpace(pgqp_memsize());
  RequestNamedLWLockTranche("pg_stat_query_plans", 1);
#endif
  prev_shmem_startup_hook = shmem_startup_hook;
  shmem_startup_hook = pgqp_shmem_startup;
  prev_post_parse_analyze_hook = post_parse_analyze_hook;
  post_parse_analyze_hook = pgqp_post_parse_analyze;
#if PG_VERSION_NUM >= 130000
  prev_planner_hook = planner_hook;
  planner_hook = pgqp_planner;
#endif
  prev_ExecutorStart = ExecutorStart_hook;
  ExecutorStart_hook = pgqp_ExecutorStart;
  prev_ExecutorRun = ExecutorRun_hook;
  ExecutorRun_hook = pgqp_ExecutorRun;
  prev_ExecutorFinish = ExecutorFinish_hook;
  ExecutorFinish_hook = pgqp_ExecutorFinish;
  prev_ExecutorEnd = ExecutorEnd_hook;
  ExecutorEnd_hook = pgqp_ExecutorEnd;
  prev_ProcessUtility = ProcessUtility_hook;
  ProcessUtility_hook = pgqp_ProcessUtility;
}

/*
 * Module unload callback
 */
void _PG_fini(void) {
  /* Uninstall hooks. */
  shmem_startup_hook = prev_shmem_startup_hook;
  post_parse_analyze_hook = prev_post_parse_analyze_hook;
  planner_hook = prev_planner_hook;
  ExecutorStart_hook = prev_ExecutorStart;
  ExecutorRun_hook = prev_ExecutorRun;
  ExecutorFinish_hook = prev_ExecutorFinish;
  ExecutorEnd_hook = prev_ExecutorEnd;
  ProcessUtility_hook = prev_ProcessUtility;
}

#if PG_VERSION_NUM >= 150000
/*
 * shmem_request hook: request additional shared resources.  We'll allocate or
 * attach to the shared resources in pgqp_shmem_startup().
 */
static void pgqp_shmem_request(void) {
  if (prev_shmem_request_hook)
    prev_shmem_request_hook();

  RequestAddinShmemSpace(pgqp_memsize());
  RequestNamedLWLockTranche("pg_stat_query_plans", 1);
}
#endif

/*
 * shmem_startup hook: allocate or attach to shared memory,
 * then load any pre-existing statistics from file.
 * Also create and load the query-texts file, which is expected to exist
 * (even if empty) while the module is enabled.
 */
static void pgqp_shmem_startup(void) {
  bool found;
  HASHCTL info_queries;
  HASHCTL info_plans;
  HASHCTL info_texts;

  if (prev_shmem_startup_hook)
    prev_shmem_startup_hook();

  /* reset in case this is a restart within the postmaster */
  pgqp = NULL;
  pgqp_queries = NULL;
  pgqp_plans = NULL;
  pgqp_texts = NULL;
  pgqp_storage = NULL;

  /*
   * Create or attach to the shared memory state, including hash table
   */
  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

  pgqp =
      ShmemInitStruct("pg_stat_query_plans", sizeof(pgqpSharedState), &found);

  if (!found) {
    /* First time through ... */
    pgqp->lock = &(GetNamedLWLockTranche("pg_stat_query_plans"))->lock;
    // pgqp->memory_lock =
    // &(GetNamedLWLockTranche("pg_stat_query_plans_memory"))->lock;
    pgqp->storage_offset = 0;
    pgqp->cur_median_usage = ASSUMED_MEDIAN_INIT;
    SpinLockInit(&pgqp->mutex);
    pgqp->stats.dealloc = 0;
    pgqp->stats.queries_wiped_out = 0;
    pgqp->stats.plans_wiped_out = 0;
    pgqp->stats.stats_reset = GetCurrentTimestamp();
    pgqp->stats.queries_size = 0;
    pgqp->stats.compressed_queries_size = 0;
    pgqp->stats.plans_size = 0;
    pgqp->stats.compressed_plans_size = 0;
    pgqp->stats.dealloc_time_ms = 0;
    pgqp->stats.gc_time_ms = 0;
  }

  info_queries.keysize = sizeof(pgqpQueryHashKey);
  info_queries.entrysize = sizeof(pgqpEntry);
  pgqp_queries = ShmemInitHash("pg_stat_query_plans hash", pgqp_max, pgqp_max,
                               &info_queries, HASH_ELEM | HASH_BLOBS);

  pgqp_max_plans = pgqp_max * MEAN_PLANS_PER_QUERY;
  info_plans.keysize = sizeof(pgqpPlanHashKey);
  info_plans.entrysize = sizeof(pgqpPlanEntry);
  pgqp_plans =
      ShmemInitHash("pg_stat_query_plans_plans hash", pgqp_max_plans,
                    pgqp_max_plans, &info_plans, HASH_ELEM | HASH_BLOBS);

  info_texts.keysize = sizeof(pgqpTextStorageKey);
  info_texts.entrysize = sizeof(pgqpTextStorageEntry);
  pgqp_texts = ShmemInitHash(
      "pg_stat_query_plans texts", pgqp_max_plans*3 + pgqp_max,
      pgqp_max_plans*3 + pgqp_max, &info_texts, HASH_ELEM | HASH_BLOBS);

  pgqp_storage = ShmemInitStruct("pg_stat_query_plans storage",
                                 pgqp_storage_memory, &found);

  if (!found) {
    /* Init storage */
    memset(pgqp_storage, 0, pgqp_storage_memory);
  }

  LWLockRelease(AddinShmemInitLock);

  return;
}

/*
 * Post-parse-analysis hook: mark query with a queryId
 */
static void
#if PG_VERSION_NUM >= 140000
pgqp_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate)
#else
pgqp_post_parse_analyze(ParseState *pstate, Query *query)
#endif
{
#if PG_VERSION_NUM < 140000
  pgqpJumbleState jstate;
#endif

  if (prev_post_parse_analyze_hook)
#if PG_VERSION_NUM >= 140000
    prev_post_parse_analyze_hook(pstate, query, jstate);
#else
    prev_post_parse_analyze_hook(pstate, query);
#endif

  /* Safety check... */
  if (!pgqp || !pgqp_queries || !pgqp_plans || !pgqp_texts ||
      !pgqp_enabled(exec_nested_level))
    return;

#if PG_VERSION_NUM < 140000
  if (query->queryId != UINT64CONST(0))
    return;
#endif

  /*
   * Clear queryId for prepared statements related utility, as those will
   * inherit from the underlying statement's one (except DEALLOCATE which is
   * entirely untracked).
   */
  if (query->utilityStmt) {
    if (pgqp_track_utility && !PGQP_HANDLED_UTILITY(query->utilityStmt))
      query->queryId = UINT64CONST(0);
    return;
  }

#if PG_VERSION_NUM < 140000
  /* Set up workspace for query jumbling */
  jstate.jumble = (unsigned char *)palloc(JUMBLE_SIZE);
  jstate.jumble_len = 0;
  jstate.clocations_buf_size = 32;
  jstate.clocations = (pgqpLocationLen *)palloc(jstate.clocations_buf_size *
                                                sizeof(pgqpLocationLen));
  jstate.clocations_count = 0;
  jstate.highest_extern_param_id = 0;

  /* Compute query ID and mark the Query node with it */
  JumbleQuery(&jstate, query);
  query->queryId =
      DatumGetUInt64(hash_any_extended(jstate.jumble, jstate.jumble_len, 0));

  /*
   * If we are unlucky enough to get a hash of zero, use 1 instead, to
   * prevent confusion with the utility-statement case.
   */
  if (query->queryId == UINT64CONST(0))
    query->queryId = UINT64CONST(1);
#endif

/*
 * If query jumbling were able to identify any ignorable constants, we
 * immediately create a hash table entry for the query, so that we can
 * record the normalized form of the query string.  If there were no such
 * constants, the normalized string would be the same as the query text
 * anyway, so there's no need for an early entry.
 */
#if PG_VERSION_NUM >= 140000
  if (jstate && jstate->clocations_count > 0)
#else
  if (jstate.clocations_count > 0)
#endif
    pgqp_store(pstate->p_sourcetext, NULL, query->queryId, NULL,
               query->stmt_location, query->stmt_len, PGQP_INVALID, 0, 0, NULL,
               NULL, NULL,
#if PG_VERSION_NUM >= 140000
               jstate
#else
               &jstate
#endif
    );
}

#if PG_VERSION_NUM >= 130000
/*
 * Planner hook: forward to regular planner, but measure planning time
 * if needed.
 */
static PlannedStmt *pgqp_planner(Query *parse, const char *query_string,
                                 int cursorOptions, ParamListInfo boundParams) {
  PlannedStmt *result;

  /*
   * We can't process the query if no query_string is provided, as
   * pgqp_store needs it.  We also ignore query without queryid, as it would
   * be treated as a utility statement, which may not be the case.
   *
   * Note that planner_hook can be called from the planner itself, so we
   * have a specific nesting level for the planner.  However, utility
   * commands containing optimizable statements can also call the planner,
   * same for regular DML (for instance for underlying foreign key queries).
   * So testing the planner nesting level only is not enough to detect real
   * top level planner call.
   */
  if (pgqp_enabled(plan_nested_level + exec_nested_level) &&
      pgqp_track_planning && query_string && parse->queryId != UINT64CONST(0)) {
    instr_time start;
    instr_time duration;
    BufferUsage bufusage_start, bufusage;
    WalUsage walusage_start, walusage;

    /* We need to track buffer usage as the planner can access them. */
    bufusage_start = pgBufferUsage;

    /*
     * Similarly the planner could write some WAL records in some cases
     * (e.g. setting a hint bit with those being WAL-logged)
     */
    walusage_start = pgWalUsage;
    INSTR_TIME_SET_CURRENT(start);

    plan_nested_level++;
    PG_TRY();
    {
      if (prev_planner_hook)
        result =
            prev_planner_hook(parse, query_string, cursorOptions, boundParams);
      else
        result =
            standard_planner(parse, query_string, cursorOptions, boundParams);
    }
    PG_FINALLY();
    { plan_nested_level--; }
    PG_END_TRY();

    INSTR_TIME_SET_CURRENT(duration);
    INSTR_TIME_SUBTRACT(duration, start);

    /* calc differences of buffer counters. */
    memset(&bufusage, 0, sizeof(BufferUsage));
    BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);

    /* calc differences of WAL counters. */
    memset(&walusage, 0, sizeof(WalUsage));
    WalUsageAccumDiff(&walusage, &pgWalUsage, &walusage_start);

    pgqp_store(query_string, NULL, parse->queryId, NULL, parse->stmt_location,
               parse->stmt_len, PGQP_PLAN, INSTR_TIME_GET_MILLISEC(duration), 0,
               &bufusage, &walusage, NULL, NULL);
  } else {
    if (prev_planner_hook)
      result =
          prev_planner_hook(parse, query_string, cursorOptions, boundParams);
    else
      result =
          standard_planner(parse, query_string, cursorOptions, boundParams);
  }

  return result;
}
#endif

/*
 * ExecutorStart hook: start up tracking if needed
 */
static void pgqp_ExecutorStart(QueryDesc *queryDesc, int eflags) {
  if (prev_ExecutorStart)
    prev_ExecutorStart(queryDesc, eflags);
  else
    standard_ExecutorStart(queryDesc, eflags);

  /*
   * If query has queryId zero, don't track it.  This prevents double
   * counting of optimizable statements that are directly contained in
   * utility statements.
   */
  if (pgqp_enabled(exec_nested_level) &&
      queryDesc->plannedstmt->queryId != UINT64CONST(0)) {
    /*
     * Set up to track total elapsed time in ExecutorRun.  Make sure the
     * space is allocated in the per-query context so it will go away at
     * ExecutorEnd.
     */
    if (queryDesc->totaltime == NULL) {
      MemoryContext oldcxt;

      oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
#if PG_VERSION_NUM >= 140000
      queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
#else
      queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
#endif
      MemoryContextSwitchTo(oldcxt);
    }
  }
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void pgqp_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
                             uint64 count, bool execute_once) {
  exec_nested_level++;
  PG_TRY();
  {
    if (prev_ExecutorRun)
      prev_ExecutorRun(queryDesc, direction, count, execute_once);
    else
      standard_ExecutorRun(queryDesc, direction, count, execute_once);
  }
#if PG_VERSION_NUM < 130000
  PG_CATCH();
  {
    exec_nested_level--;
    PG_RE_THROW();
  }
#else
  PG_FINALLY();
  { exec_nested_level--; }
#endif
  PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void pgqp_ExecutorFinish(QueryDesc *queryDesc) {
  exec_nested_level++;
  PG_TRY();
  {
    if (prev_ExecutorFinish)
      prev_ExecutorFinish(queryDesc);
    else
      standard_ExecutorFinish(queryDesc);
  }
#if PG_VERSION_NUM < 130000
  PG_CATCH();
  {
    exec_nested_level--;
    PG_RE_THROW();
  }
#else
  PG_FINALLY();
  { exec_nested_level--; }
#endif
  PG_END_TRY();
}

/*
 * ExecutorEnd hook: store results if needed
 */
static void pgqp_ExecutorEnd(QueryDesc *queryDesc) {
  uint64 queryId = queryDesc->plannedstmt->queryId;
  ExplainState *es_planid;
  StringInfo execution_plan = NULL;
  MemoryContext myctx;
  MemoryContext oldctx;

  if (queryId != UINT64CONST(0) && queryDesc->totaltime &&
      pgqp_enabled(exec_nested_level)) {
    /*
     * Make sure stats accumulation is done.  (Note: it's okay if several
     * levels of hook all do this.)
     */
    InstrEndLoop(queryDesc->totaltime);

    /*
     * Create new memory context to store intermediate data
     */
    myctx = AllocSetContextCreate(CurrentMemoryContext, "PgssExecutorEndMemCtx", ALLOCSET_DEFAULT_SIZES);
    oldctx = MemoryContextSwitchTo(myctx);

    /*
     * Calculate plan id value as hash os EXPLAIN PLAN text representation
     * without costs and the number of rows. Changing costs does not mean
     * changing execution plan - it could be changing statistics. Cost, the
     * number of rows,width and other params  will be stored in additional
     * fields.
     */

    if (pgqp_track_plans) {
      es_planid = NewExplainState();
      es_planid->costs = false;
      es_planid->verbose = true;
      es_planid->format = EXPLAIN_FORMAT_TEXT;
      ExplainBeginOutput(es_planid);
      ExplainPrintPlan(es_planid, queryDesc);
      ExplainEndOutput(es_planid);
      if (pgqp_normalize_plans) {
        execution_plan = gen_normplan(es_planid->str->data);
      } else
        execution_plan = es_planid->str;
    }

    pgqp_store(
        queryDesc->sourceText, execution_plan, queryId, queryDesc,
        queryDesc->plannedstmt->stmt_location, queryDesc->plannedstmt->stmt_len,
        PGQP_EXEC, queryDesc->totaltime->total * 1000.0, /* convert to msec */
        queryDesc->estate->es_processed, &queryDesc->totaltime->bufusage,
#if PG_VERSION_NUM >= 130000
        &queryDesc->totaltime->walusage,
#else
        NULL,
#endif
        queryDesc->estate->es_jit ? &queryDesc->estate->es_jit->instr : NULL,
        NULL);

    /*
     * Free context memory
     */
    MemoryContextSwitchTo(oldctx);
    MemoryContextDelete(myctx);
  }

  if (prev_ExecutorEnd)
    prev_ExecutorEnd(queryDesc);
  else
    standard_ExecutorEnd(queryDesc);
}

#if PG_VERSION_NUM >= 140000
/*
 * ProcessUtility hook
 */
static void pgqp_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                                bool readOnlyTree,
                                ProcessUtilityContext context,
                                ParamListInfo params,
                                QueryEnvironment *queryEnv, DestReceiver *dest,
                                QueryCompletion *qc) {
  Node *parsetree = pstmt->utilityStmt;
  uint64 saved_queryId = pstmt->queryId;

  /*
   * Force utility statements to get queryId zero.  We do this even in cases
   * where the statement contains an optimizable statement for which a
   * queryId could be derived (such as EXPLAIN or DECLARE CURSOR).  For such
   * cases, runtime control will first go through ProcessUtility and then
   * the executor, and we don't want the executor hooks to do anything,
   * since we are already measuring the statement's costs at the utility
   * level.
   *
   * Note that this is only done if pg_stat_query_plans is enabled and
   * configured to track utility statements, in the unlikely possibility
   * that user configured another extension to handle utility statements
   * only.
   */
  if (pgqp_enabled(exec_nested_level) && pgqp_track_utility)
    pstmt->queryId = UINT64CONST(0);

  /*
   * If it's an EXECUTE statement, we don't track it and don't increment the
   * nesting level.  This allows the cycles to be charged to the underlying
   * PREPARE instead (by the Executor hooks), which is much more useful.
   *
   * We also don't track execution of PREPARE.  If we did, we would get one
   * hash table entry for the PREPARE (with hash calculated from the query
   * string), and then a different one with the same query string (but hash
   * calculated from the query tree) would be used to accumulate costs of
   * ensuing EXECUTEs.  This would be confusing, and inconsistent with other
   * cases where planning time is not included at all.
   *
   * Likewise, we don't track execution of DEALLOCATE.
   */
  if (pgqp_track_utility && pgqp_enabled(exec_nested_level) &&
      PGQP_HANDLED_UTILITY(parsetree)) {
    instr_time start;
    instr_time duration;
    uint64 rows;
    BufferUsage bufusage_start, bufusage;
    WalUsage walusage_start, walusage;

    bufusage_start = pgBufferUsage;
    walusage_start = pgWalUsage;
    INSTR_TIME_SET_CURRENT(start);

    exec_nested_level++;
    PG_TRY();
    {
      if (prev_ProcessUtility)
        prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
                            queryEnv, dest, qc);
      else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
                                params, queryEnv, dest, qc);
    }
    PG_FINALLY();
    { exec_nested_level--; }
    PG_END_TRY();

    INSTR_TIME_SET_CURRENT(duration);
    INSTR_TIME_SUBTRACT(duration, start);

    /*
     * Track the total number of rows retrieved or affected by the utility
     * statements of COPY, FETCH, CREATE TABLE AS, CREATE MATERIALIZED
     * VIEW, REFRESH MATERIALIZED VIEW and SELECT INTO.
     */
    rows = (qc &&
            (qc->commandTag == CMDTAG_COPY || qc->commandTag == CMDTAG_FETCH ||
             qc->commandTag == CMDTAG_SELECT ||
             qc->commandTag == CMDTAG_REFRESH_MATERIALIZED_VIEW))
               ? qc->nprocessed
               : 0;

    /* calc differences of buffer counters. */
    memset(&bufusage, 0, sizeof(BufferUsage));
    BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);

    /* calc differences of WAL counters. */
    memset(&walusage, 0, sizeof(WalUsage));
    WalUsageAccumDiff(&walusage, &pgWalUsage, &walusage_start);

    pgqp_store(queryString, NULL, saved_queryId, NULL, pstmt->stmt_location,
               pstmt->stmt_len, PGQP_EXEC, INSTR_TIME_GET_MILLISEC(duration),
               rows, &bufusage, &walusage, NULL, NULL);
  } else {
    if (prev_ProcessUtility)
      prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
                          queryEnv, dest, qc);
    else
      standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
                              queryEnv, dest, qc);
  }
}
#else
/*
 * ProcessUtility hook
 */
static void pgqp_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
                                ProcessUtilityContext context,
                                ParamListInfo params,
                                QueryEnvironment *queryEnv, DestReceiver *dest,
#if PG_VERSION_NUM >= 130000
                                QueryCompletion *qc
#else
                                char *qc
#endif
) {
  Node *parsetree = pstmt->utilityStmt;

  /*
   * If it's an EXECUTE statement, we don't track it and don't increment the
   * nesting level.  This allows the cycles to be charged to the underlying
   * PREPARE instead (by the Executor hooks), which is much more useful.
   *
   * We also don't track execution of PREPARE.  If we did, we would get one
   * hash table entry for the PREPARE (with hash calculated from the query
   * string), and then a different one with the same query string (but hash
   * calculated from the query tree) would be used to accumulate costs of
   * ensuing EXECUTEs.  This would be confusing, and inconsistent with other
   * cases where planning time is not included at all.
   *
   * Likewise, we don't track execution of DEALLOCATE.
   */
  if (pgqp_track_utility && pgqp_enabled(exec_nested_level) &&
      !IsA(parsetree, ExecuteStmt) && !IsA(parsetree, PrepareStmt) &&
      !IsA(parsetree, DeallocateStmt)) {
    instr_time start;
    instr_time duration;
    uint64 rows;
    BufferUsage bufusage_start, bufusage;
#if PG_VERSION_NUM >= 130000
    WalUsage walusage_start, walusage;
#endif

    bufusage_start = pgBufferUsage;
#if PG_VERSION_NUM >= 130000
    walusage_start = pgWalUsage;
#endif
    INSTR_TIME_SET_CURRENT(start);
    exec_nested_level++;
    PG_TRY();
    {
      if (prev_ProcessUtility)
        prev_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest,
                            qc);
      else
        standard_ProcessUtility(pstmt, queryString, context, params, queryEnv,
                                dest, qc);
    }
#if PG_VERSION_NUM < 130000
    PG_CATCH();
    {
      exec_nested_level--;
      PG_RE_THROW();
    }
#else
    PG_FINALLY();
    { exec_nested_level--; }
#endif
    PG_END_TRY();

    INSTR_TIME_SET_CURRENT(duration);
    INSTR_TIME_SUBTRACT(duration, start);

#if PG_VERSION_NUM >= 130000
    rows = (qc && qc->commandTag == CMDTAG_COPY) ? qc->nprocessed : 0;

    /* calc differences of buffer counters. */
    memset(&bufusage, 0, sizeof(BufferUsage));
    BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);

    /* calc differences of WAL counters. */
    memset(&walusage, 0, sizeof(WalUsage));
    WalUsageAccumDiff(&walusage, &pgWalUsage, &walusage_start);
#else
    /* parse command tag to retrieve the number of affected rows. */
    if (qc && strncmp(qc, "COPY ", 5) == 0)
      rows = pg_strtouint64(qc + 5, NULL, 10);
    else
      rows = 0;

    /* calc differences of buffer counters. */
    bufusage.shared_blks_hit =
        pgBufferUsage.shared_blks_hit - bufusage_start.shared_blks_hit;
    bufusage.shared_blks_read =
        pgBufferUsage.shared_blks_read - bufusage_start.shared_blks_read;
    bufusage.shared_blks_dirtied =
        pgBufferUsage.shared_blks_dirtied - bufusage_start.shared_blks_dirtied;
    bufusage.shared_blks_written =
        pgBufferUsage.shared_blks_written - bufusage_start.shared_blks_written;
    bufusage.local_blks_hit =
        pgBufferUsage.local_blks_hit - bufusage_start.local_blks_hit;
    bufusage.local_blks_read =
        pgBufferUsage.local_blks_read - bufusage_start.local_blks_read;
    bufusage.local_blks_dirtied =
        pgBufferUsage.local_blks_dirtied - bufusage_start.local_blks_dirtied;
    bufusage.local_blks_written =
        pgBufferUsage.local_blks_written - bufusage_start.local_blks_written;
    bufusage.temp_blks_read =
        pgBufferUsage.temp_blks_read - bufusage_start.temp_blks_read;
    bufusage.temp_blks_written =
        pgBufferUsage.temp_blks_written - bufusage_start.temp_blks_written;
    bufusage.blk_read_time = pgBufferUsage.blk_read_time;
    INSTR_TIME_SUBTRACT(bufusage.blk_read_time, bufusage_start.blk_read_time);
    bufusage.blk_write_time = pgBufferUsage.blk_write_time;
    INSTR_TIME_SUBTRACT(bufusage.blk_write_time, bufusage_start.blk_write_time);
#endif

    pgqp_store(queryString, NULL, 0, /* signal that it's a utility stmt */
               NULL, pstmt->stmt_location, pstmt->stmt_len, PGQP_EXEC,
               INSTR_TIME_GET_MILLISEC(duration), rows, &bufusage,
#if PG_VERSION_NUM >= 130000
               &walusage,
#else
               NULL,
#endif
               NULL, NULL);
  } else {
    if (prev_ProcessUtility)
      prev_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest,
                          qc);
    else
      standard_ProcessUtility(pstmt, queryString, context, params, queryEnv,
                              dest, qc);
  }
}
#endif

/*
 * Reset statement statistics corresponding to userid, dbid, and queryid.
 */
Datum pg_stat_query_plans_reset_queryid(PG_FUNCTION_ARGS) {
  Oid userid;
  Oid dbid;
  uint64 queryid;

  userid = PG_GETARG_OID(0);
  dbid = PG_GETARG_OID(1);
  queryid = (uint64)PG_GETARG_INT64(2);

  pgqp_entry_reset(userid, dbid, queryid);

  PG_RETURN_VOID();
}

/*
 * Reset minimum and maximum value - usefull if we do not want to reset all stat
 * and create then new entries in hash table once again
 */
Datum pg_stat_query_plans_reset_minmax(PG_FUNCTION_ARGS) {
  HASH_SEQ_STATUS hash_seq;
  pgqpEntry *entry;
  pgqpPlanEntry *plan_entry;

  /* hash table must exist already */
  if (!pgqp || !pgqp_queries || !pgqp_plans || !pgqp_texts)
    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("pg_stat_query_plans must be loaded via "
                           "shared_preload_libraries")));

  LWLockAcquire(pgqp->lock, LW_SHARED);

  /* Reset queries stat */

  hash_seq_init(&hash_seq, pgqp_queries);
  while ((entry = hash_seq_search(&hash_seq)) != NULL) {
    volatile pgqpEntry *e = (volatile pgqpEntry *)entry;
    SpinLockAcquire(&e->mutex);
    for (int kind = 0; kind < PGQP_NUMKIND; kind++) {
      e->counters.min_time[kind] = 0;
      e->counters.max_time[kind] = 0;
      e->counters.mean_time[kind] = 0;
      e->counters.sum_var_time[kind] = 0;
    }
    SpinLockRelease(&e->mutex);
  }

  /* Reset plans stat */

  hash_seq_init(&hash_seq, pgqp_plans);
  while ((plan_entry = hash_seq_search(&hash_seq)) != NULL) {
    volatile pgqpPlanEntry *e = (volatile pgqpPlanEntry *)plan_entry;
    SpinLockAcquire(&e->mutex);
    for (int kind = 0; kind < PGQP_NUMKIND; kind++) {
      e->counters.min_time[kind] = 0;
      e->counters.max_time[kind] = 0;
      e->counters.mean_time[kind] = 0;
      e->counters.sum_var_time[kind] = 0;
    }
    SpinLockRelease(&e->mutex);
  }

  LWLockRelease(pgqp->lock);

  PG_RETURN_VOID();
}

#define PG_STAT_QUERY_PLANS_SQL_COLS_V1_0 44
#define PG_STAT_QUERY_PLANS_SQL_COLS 44 /* maximum of above */

#define PG_STAT_QUERY_PLANS_PLAN_COLS_V1_0 45
#define PG_STAT_QUERY_PLANS_PLAN_COLS 45 /* maximum of above */

static int pgqp_add_counters_data(Counters *c, Datum values[0],
                                  pgqpStoreKind start_kind) {
  int i = 0;
  double stddev;
  char buf[256];
  Datum wal_bytes;

  for (pgqpStoreKind kind = start_kind; kind < PGQP_NUMKIND; kind++) {
    values[i++] = Int64GetDatumFast(c->calls[kind]);
    values[i++] = Float8GetDatumFast(c->total_time[kind]);

    values[i++] = Float8GetDatumFast(c->min_time[kind]);
    values[i++] = Float8GetDatumFast(c->max_time[kind]);
    values[i++] = Float8GetDatumFast(c->mean_time[kind]);

    /*
     * Note we are calculating the population variance here, not
     * the sample variance, as we have data for the whole
     * population, so Bessel's correction is not used, and we
     * don't divide by tmp.calls - 1.
     */
    if (c->calls[kind] > 1)
      stddev = sqrt(c->sum_var_time[kind] / c->calls[kind]);
    else
      stddev = 0.0;
    values[i++] = Float8GetDatumFast(stddev);
  }
  values[i++] = Int64GetDatumFast(c->rows);
  values[i++] = Int64GetDatumFast(c->shared_blks_hit);
  values[i++] = Int64GetDatumFast(c->shared_blks_read);
  values[i++] = Int64GetDatumFast(c->shared_blks_dirtied);
  values[i++] = Int64GetDatumFast(c->shared_blks_written);
  values[i++] = Int64GetDatumFast(c->local_blks_hit);
  values[i++] = Int64GetDatumFast(c->local_blks_read);
  values[i++] = Int64GetDatumFast(c->local_blks_dirtied);
  values[i++] = Int64GetDatumFast(c->local_blks_written);
  values[i++] = Int64GetDatumFast(c->temp_blks_read);
  values[i++] = Int64GetDatumFast(c->temp_blks_written);
  values[i++] = Float8GetDatumFast(c->blk_read_time);
  values[i++] = Float8GetDatumFast(c->blk_write_time);
  values[i++] = Float8GetDatumFast(c->temp_blk_read_time);
  values[i++] = Float8GetDatumFast(c->temp_blk_write_time);
  values[i++] = Int64GetDatumFast(c->wal_records);
  values[i++] = Int64GetDatumFast(c->wal_fpi);

  snprintf(buf, sizeof buf, UINT64_FORMAT, c->wal_bytes);

  /* Convert to numeric. */
  wal_bytes = DirectFunctionCall3(numeric_in, CStringGetDatum(buf),
                                  ObjectIdGetDatum(0), Int32GetDatum(-1));
  values[i++] = wal_bytes;

  values[i++] = Int64GetDatumFast(c->jit_functions);
  values[i++] = Float8GetDatumFast(c->jit_generation_time);
  values[i++] = Int64GetDatumFast(c->jit_inlining_count);
  values[i++] = Float8GetDatumFast(c->jit_inlining_time);
  values[i++] = Int64GetDatumFast(c->jit_optimization_count);
  values[i++] = Float8GetDatumFast(c->jit_optimization_time);
  values[i++] = Int64GetDatumFast(c->jit_emission_count);
  values[i++] = Float8GetDatumFast(c->jit_emission_time);

  return i;
}

/*
 * Retrieve execution plan statistics
 */
Datum pg_stat_query_plans_plans_1_0(PG_FUNCTION_ARGS) {
  bool showtext = PG_GETARG_BOOL(0);

  pg_stat_query_plans_plan_internal(fcinfo, PGQP_V1_0, showtext);

  return (Datum)0;
}

/* Common code for all versions of pg_stat_query_plans_plans() */
static void pg_stat_query_plans_plan_internal(FunctionCallInfo fcinfo,
                                                pgqpVersion api_version,
                                                bool showtext) {
  ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
  Oid userid = GetUserId();
  bool is_allowed_role = false;
#if PG_VERSION_NUM < 150000
  MemoryContext per_query_ctx;
  MemoryContext oldcontext;
  TupleDesc tupdesc;
  Tuplestorestate *tupstore;
#endif
  HASH_SEQ_STATUS hash_seq;
  pgqpPlanEntry *entry;

  /* hash table must exist already */
  if (!pgqp || !pgqp_queries || !pgqp_plans || !pgqp_texts)
    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("pg_stat_query_plans must be loaded via "
                           "shared_preload_libraries")));

#if PG_VERSION_NUM >= 150000
  InitMaterializedSRF(fcinfo, 0);

  switch (rsinfo->setDesc->natts)
#else
  per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
  oldcontext = MemoryContextSwitchTo(per_query_ctx);
  /* Build a tuple descriptor for our result type */
  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    elog(ERROR, "return type must be a row type");

  switch (tupdesc->natts)
#endif
  {
  case PG_STAT_QUERY_PLANS_PLAN_COLS_V1_0:
    if (api_version != PGQP_V1_0)
      elog(ERROR, "incorrect number of output arguments");
    break;
  default:
    elog(ERROR, "incorrect number of output arguments");
  }

#if PG_VERSION_NUM < 150000
  tupstore = tuplestore_begin_heap(true, false, work_mem);
  rsinfo->returnMode = SFRM_Materialize;
  rsinfo->setResult = tupstore;
  rsinfo->setDesc = tupdesc;

  MemoryContextSwitchTo(oldcontext);
#endif

  /*
   * Get shared lock, load or reload the query text file if we must, and
   * iterate over the hashtable entries.
   */
  LWLockAcquire(pgqp->lock, LW_SHARED);

  hash_seq_init(&hash_seq, pgqp_plans);
  while ((entry = hash_seq_search(&hash_seq)) != NULL) {
    Datum values[PG_STAT_QUERY_PLANS_PLAN_COLS];
    bool nulls[PG_STAT_QUERY_PLANS_PLAN_COLS];
    int i = 0;
    Counters tmp;
    PlanCounters tmp_plan;
    char *tmp_str;
    volatile pgqpTextStorageEntry *s_query =
        (volatile pgqpTextStorageEntry *)entry->query_text;
    volatile pgqpTextStorageEntry *s_ex =
        (volatile pgqpTextStorageEntry *)entry->example_plan;
    volatile pgqpTextStorageEntry *s_gen =
        (volatile pgqpTextStorageEntry *)entry->gen_plan;

    /* Don't show anything if not have rights */
    if (!is_allowed_role && entry->key.userid != userid) {
      elog(DEBUG1, "Do not have rights to view %lu", entry->key.queryid);
      // continue;
    }

    memset(values, 0, sizeof(values));
    memset(nulls, 0, sizeof(nulls));

    values[i++] = ObjectIdGetDatum(entry->key.userid);
    values[i++] = ObjectIdGetDatum(entry->key.dbid);
    values[i++] = BoolGetDatum(entry->key.toplevel);
    values[i++] = UInt64GetDatum(entry->key.queryid);
    values[i++] = UInt64GetDatum(entry->key.planid);

    if (showtext && s_query && s_query->usage_count > 0) {
      tmp_str = get_decoded_text(s_query);
      values[i++] = CStringGetTextDatum(tmp_str);
      if (s_query->text_encoding != PGQP_PLAINTEXT)
        pfree(tmp_str);
    } else
      nulls[i++] = true;

    if (showtext && s_gen && s_gen->usage_count > 0) {
      tmp_str = get_decoded_text(s_gen);
      values[i++] = CStringGetTextDatum(tmp_str);
      if (s_gen->text_encoding != PGQP_PLAINTEXT)
        pfree(tmp_str);
    } else
      nulls[i++] = true;

    if (showtext && s_ex && s_ex->usage_count > 0) {
      tmp_str = get_decoded_text(s_ex);
      values[i++] = CStringGetTextDatum(tmp_str);
      if (s_ex->text_encoding != PGQP_PLAINTEXT)
        pfree(tmp_str);
    } else
      nulls[i++] = true;

    /* copy counters to a local variable to keep locking time short */
    {
      volatile pgqpPlanEntry *e = (volatile pgqpPlanEntry *)entry;
      SpinLockAcquire(&e->mutex);
      tmp = e->counters;
      tmp_plan = e->plan_counters;
      SpinLockRelease(&e->mutex);
    }

    i += pgqp_add_counters_data(&tmp, &values[i], PGQP_EXEC);

    values[i++] = Float8GetDatumFast(tmp_plan.startup_cost);
    values[i++] = Float8GetDatumFast(tmp_plan.total_cost);
    values[i++] = UInt64GetDatum(tmp_plan.plan_rows);
    values[i++] = UInt64GetDatum(tmp_plan.plan_width);

    values[i++] = Int64GetDatumFast(entry->generation);

    Assert(i == (api_version == PGQP_V1_0
                     ? PG_STAT_QUERY_PLANS_PLAN_COLS_V1_0
                     : -1 /* fail if you forget to update this assert */));

#if PG_VERSION_NUM >= 150000
    tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
#else
    tuplestore_putvalues(tupstore, tupdesc, values, nulls);
#endif
  }

  LWLockRelease(pgqp->lock);

#if PG_VERSION_NUM < 150000
  tuplestore_donestoring(tupstore);
#endif
}

/*
 * Retrieve statement statistics.
 */
Datum pg_stat_query_plans_sql_1_0(PG_FUNCTION_ARGS) {
  bool showtext = PG_GETARG_BOOL(0);

  pg_stat_query_plans_stat_internal(fcinfo, PGQP_V1_0, showtext);

  return (Datum)0;
}

/* Common code for all versions of pg_stat_query_plans() */
static void pg_stat_query_plans_stat_internal(FunctionCallInfo fcinfo,
                                                pgqpVersion api_version,
                                                bool showtext) {
  ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
#if PG_VERSION_NUM < 150000
  MemoryContext per_query_ctx;
  MemoryContext oldcontext;
  TupleDesc tupdesc;
  Tuplestorestate *tupstore;
#endif
  Oid userid = GetUserId();
  bool is_allowed_role = false;
  HASH_SEQ_STATUS hash_seq;
  pgqpEntry *entry;

/*
 * Superusers or roles with the privileges of pg_read_all_stats members
 * are allowed
 */
#if PG_VERSION_NUM >= 140000
  is_allowed_role = has_privs_of_role(userid, ROLE_PG_READ_ALL_STATS);
#else
  is_allowed_role = is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_STATS);
#endif

  /* hash table must exist already */
  if (!pgqp || !pgqp_queries || !pgqp_texts)
    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("pg_stat_query_plans must be loaded via "
                           "shared_preload_libraries")));

#if PG_VERSION_NUM >= 150000
  InitMaterializedSRF(fcinfo, 0);

  switch (rsinfo->setDesc->natts)
#else
  per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
  oldcontext = MemoryContextSwitchTo(per_query_ctx);
  /* Build a tuple descriptor for our result type */
  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    elog(ERROR, "return type must be a row type");

  switch (tupdesc->natts)
#endif
  {
  case PG_STAT_QUERY_PLANS_SQL_COLS_V1_0:
    if (api_version != PGQP_V1_0)
      elog(ERROR, "incorrect number of output arguments");
    break;
  default:
    elog(ERROR, "incorrect number of output arguments");
  }

#if PG_VERSION_NUM < 150000
  tupstore = tuplestore_begin_heap(true, false, work_mem);
  rsinfo->returnMode = SFRM_Materialize;
  rsinfo->setResult = tupstore;
  rsinfo->setDesc = tupdesc;

  MemoryContextSwitchTo(oldcontext);
#endif

  /*
   * Get shared lock to iterate over the hashtable entries.
   *
   * With a large hash table, we might be holding the lock rather longer
   * than one could wish.  However, this only blocks creation of new hash
   * table entries, and the larger the hash table the less likely that is to
   * be needed.  So we can hope this is okay.  Perhaps someday we'll decide
   * we need to partition the hash table to limit the time spent holding any
   * one lock.
   */
  LWLockAcquire(pgqp->lock, LW_SHARED);

  hash_seq_init(&hash_seq, pgqp_queries);
  while ((entry = hash_seq_search(&hash_seq)) != NULL) {
    Datum values[PG_STAT_QUERY_PLANS_SQL_COLS];
    bool nulls[PG_STAT_QUERY_PLANS_SQL_COLS];
    int i = 0;
    Counters tmp;
    Counters *tmpp;
    volatile pgqpTextStorageEntry *s =
        (volatile pgqpTextStorageEntry *)entry->query_text;

    /* Don't show anything if not have rights */
    if (!is_allowed_role && entry->key.userid != userid) {
      elog(DEBUG1, "Do not have rights to view %lu", entry->key.queryid);
      continue;
    }

    memset(values, 0, sizeof(values));
    memset(nulls, 0, sizeof(nulls));

    values[i++] = ObjectIdGetDatum(entry->key.userid);
    values[i++] = ObjectIdGetDatum(entry->key.dbid);
    values[i++] = BoolGetDatum(entry->key.toplevel);

    values[i++] = UInt64GetDatum(entry->key.queryid);

    if (showtext && s && s->usage_count > 0) {
      char *tmp_str = get_decoded_text(s);
      values[i++] = CStringGetTextDatum(tmp_str);
      if (s->text_encoding != PGQP_PLAINTEXT)
        pfree(tmp_str);
    } else {
      nulls[i++] = true;
    }

    /* copy counters to a local variable to keep locking time short */
    {
      volatile pgqpEntry *e = (volatile pgqpEntry *)entry;

      SpinLockAcquire(&e->mutex);
      tmp = e->counters;
      SpinLockRelease(&e->mutex);
    }

    /* Skip entry if unexecuted (ie, it's a pending "sticky" entry) */
    tmpp = &tmp;
    if (IS_STICKY(tmpp)) {
      continue;
    }

    i += pgqp_add_counters_data(&tmp, &values[i], PGQP_PLAN);

    values[i++] = Int64GetDatumFast(entry->generation);

    Assert(i == (api_version == PGQP_V1_0
                     ? PG_STAT_QUERY_PLANS_SQL_COLS_V1_0
                     : -1 /* fail if you forget to update this assert */));

#if PG_VERSION_NUM >= 150000
    tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
#else
    tuplestore_putvalues(tupstore, tupdesc, values, nulls);
#endif
  }

  LWLockRelease(pgqp->lock);

#if PG_VERSION_NUM < 150000
  tuplestore_donestoring(tupstore);
#endif
}

/* Number of output arguments (columns) for pg_stat_query_plans_info */
#define PG_STAT_QUERY_PLANS_INFO_COLS 15

/*
 * Return statistics of pg_stat_query_plans.
 */
Datum pg_stat_query_plans_info(PG_FUNCTION_ARGS) {
  TupleDesc tupdesc;
  Datum values[PG_STAT_QUERY_PLANS_INFO_COLS];
  bool nulls[PG_STAT_QUERY_PLANS_INFO_COLS];

  if (!pgqp || !pgqp_queries || !pgqp_plans || !pgqp_texts)
    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("pg_stat_query_plans must be loaded via "
                           "shared_preload_libraries")));

  /* Build a tuple descriptor for our result type */
  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
    elog(ERROR, "return type must be a row type");

  MemSet(values, 0, sizeof(values));
  MemSet(nulls, 0, sizeof(nulls));

  /* Read global statistics for pg_stat_query_plans */
  {
    volatile pgqpSharedState *s = (volatile pgqpSharedState *)pgqp;

    LWLockAcquire(pgqp->lock, LW_SHARED);
    SpinLockAcquire(&s->mutex);
    values[0] = Int64GetDatum(s->stats.dealloc);
    values[1] = TimestampTzGetDatum(s->stats.stats_reset);
    values[2] = Int64GetDatum(s->gc_count);
    values[3] = Int64GetDatum(s->generation);
    values[4] = Int64GetDatum(s->storage_offset);
    values[5] = Int64GetDatum(s->stats.queries_size);
    values[6] = Int64GetDatum(s->stats.compressed_queries_size);
    values[7] = Int64GetDatum(s->stats.plans_size);
    values[8] = Int64GetDatum(s->stats.compressed_plans_size);
    values[9] = Int64GetDatum(hash_get_num_entries(pgqp_queries));
    values[10] = Int64GetDatum(hash_get_num_entries(pgqp_plans));
    values[11] = Int64GetDatum(s->stats.queries_wiped_out);
    values[12] = Int64GetDatum(s->stats.plans_wiped_out);
    values[13] = Int64GetDatum(s->stats.dealloc_time_ms);
    values[14] = Int64GetDatum(s->stats.gc_time_ms);
    SpinLockRelease(&s->mutex);
    LWLockRelease(pgqp->lock);
  }

  PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * Estimate shared memory space needed.
 */
static Size pgqp_memsize(void) {
  Size size;

  size = MAXALIGN(sizeof(pgqpSharedState));
  size = add_size(size, hash_estimate_size(pgqp_max, sizeof(pgqpEntry)));
  size = add_size(size, hash_estimate_size(pgqp_max * MEAN_PLANS_PER_QUERY,
                                           sizeof(pgqpPlanEntry)));
  size = add_size(size,
                  hash_estimate_size(pgqp_max + pgqp_max * MEAN_PLANS_PER_QUERY * 3,
                                     sizeof(pgqpTextStorageEntry)));
  size = add_size(size, MAXALIGN(pgqp_storage_memory));

  return size;
}
