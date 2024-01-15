/*-------------------------------------------------------------------------
 *
 * pg_stat_query_plans.c
 *		Track statement planning and execution times as well as resource
 *		usage across a whole database cluster.
 *
 * Common structures and type definitions
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifndef PGSTATSTATEMENSYA_H
#define PGSTATSTATEMENSYA_H
/* Location of permanent stats file (valid when database is shut down) */
#define USAGE_EXEC(duration)	(1.0)
#define USAGE_INIT				(1.0)	/* including initial planning */
#define ASSUMED_MEDIAN_INIT		(10.0)	/* initial assumed median usage */
#define ASSUMED_LENGTH_INIT		1024	/* initial assumed mean query length */
#define USAGE_DECREASE_FACTOR	(0.99)	/* decreased every entry_dealloc */
#define STICKY_DECREASE_FACTOR	(0.50)	/* factor for sticky entries */
#define USAGE_DEALLOC_PERCENT	5		/* free this % of entries at once */
#define FREE_PERCENT			10		/* % of free entries in pgqp_hash after deallocation */
#define ID_SEEDVALUE		42		/* seed value for calculate planid hash
value */
#define IS_STICKY(c)	((c->calls[PGQP_PLAN] + c->calls[PGQP_EXEC]) == 0)

/*
 * Utility statements that pgqp_ProcessUtility and pgqp_post_parse_analyze
 * ignores.
 */
#define PGQP_HANDLED_UTILITY(n)		(!IsA(n, ExecuteStmt) && \
									!IsA(n, PrepareStmt) && \
									!IsA(n, DeallocateStmt))

/*
 * Extension version number, for supporting older extension versions' objects
 */
typedef enum pgqpVersion
{
	PGQP_V1_0 = 0
} pgqpVersion;

typedef enum pgqpStoreKind
{
	PGQP_INVALID = -1,

	/*
	 * PGQP_PLAN and PGQP_EXEC must be respectively 0 and 1 as they're used to
	 * reference the underlying values in the arrays in the Counters struct,
	 * and this order is required in pg_stat_query_plans_internal().
	 */
	PGQP_PLAN = 0,
	PGQP_EXEC,

	PGQP_NUMKIND				/* Must be last value of this enum */
} pgqpStoreKind;

/*
 * Hashtable key that defines the identity of a hashtable entry.  We separate
 * queries by user and by database even if they are otherwise identical.
 */
typedef struct pgqpQueryHashKey
{
	Oid		    userid;			/* user OID */
	Oid		    dbid;			/* database OID */
	uint64		queryid;		/* query identifier */
	bool		toplevel;		/* query executed at top level */
} pgqpQueryHashKey;

typedef struct pgqpPlanHashKey
{
	Oid         userid;         /* user OID */
	Oid         dbid;           /* database OID */
	uint64      queryid;        /* query identifier */
	bool        toplevel;       /* query executed at top level */
	uint64      planid;         /* execution plan identifier */
} pgqpPlanHashKey;

/*
 * The actual stats counters kept within pgqpEntry.
 */
typedef struct Counters
{
	int64		calls[PGQP_NUMKIND];	/* # of times planned/executed */
	double		total_time[PGQP_NUMKIND];	/* total planning/execution time,
											 * in msec */
	double		min_time[PGQP_NUMKIND]; /* minimum planning/execution time in
										 * msec */
	double		max_time[PGQP_NUMKIND]; /* maximum planning/execution time in
										 * msec */
	double		mean_time[PGQP_NUMKIND];	/* mean planning/execution time in
											 * msec */
	double		sum_var_time[PGQP_NUMKIND]; /* sum of variances in
											 * planning/execution time in msec */
	int64		rows;			/* total # of retrieved or affected rows */
	int64		shared_blks_hit;	/* # of shared buffer hits */
	int64		shared_blks_read;	/* # of shared disk blocks read */
	int64		shared_blks_dirtied;	/* # of shared disk blocks dirtied */
	int64		shared_blks_written;	/* # of shared disk blocks written */
	int64		local_blks_hit; /* # of local buffer hits */
	int64		local_blks_read;	/* # of local disk blocks read */
	int64		local_blks_dirtied; /* # of local disk blocks dirtied */
	int64		local_blks_written; /* # of local disk blocks written */
	int64		temp_blks_read; /* # of temp blocks read */
	int64		temp_blks_written;	/* # of temp blocks written */
	double		blk_read_time;	/* time spent reading blocks, in msec */
	double		blk_write_time; /* time spent writing blocks, in msec */
	double		temp_blk_read_time; /* time spent reading temp blocks, in msec */
	double		temp_blk_write_time;	/* time spent writing temp blocks, in
										 * msec */
	double		usage;			/* usage factor */
	int64		wal_records;	/* # of WAL records generated */
	int64		wal_fpi;		/* # of WAL full page images generated */
	uint64		wal_bytes;		/* total amount of WAL generated in bytes */
	int64		jit_functions;	/* total number of JIT functions emitted */
	double		jit_generation_time;	/* total time to generate jit code */
	int64		jit_inlining_count; /* number of times inlining time has been
									 * > 0 */
	double		jit_inlining_time;	/* total time to inline jit code */
	int64		jit_optimization_count; /* number of times optimization time
										 * has been > 0 */
	double		jit_optimization_time;	/* total time to optimize jit code */
	int64		jit_emission_count; /* number of times emission time has been
									 * > 0 */
	double		jit_emission_time;	/* total time to emit jit code */
} Counters;

/*
 * Stats specific for query execution plans
 */
typedef struct PlanCounters
{
	Cost 		startup_cost;	/* cost before the first row can be returned */
	Cost 		total_cost;		/* cost to return all the rows */
	#if PG_VERSION_NUM >= 150000
	Cardinality plan_rows;		/* estimated count of returned rows */
	#else
	double		plan_rows;      /* estimated count of returned rows */
	#endif
	int 		plan_width;		/* estimated width of each row */
} PlanCounters;

/*
 * Global statistics for pg_stat_query_plans
 */
typedef struct pgqpGlobalStats
{
	int64		dealloc;			/* # of times entries were deallocated */
	TimestampTz stats_reset;		/* timestamp with all stats reset */
	int64		queries_size;		/* total size of queries */
	int64       compressed_queries_size; /* amount of memory consumed by queries */
	int64		plans_size;			/* total size of execution plans */
	int64       compressed_plans_size;   /* amount of memory consumed by query execution plans */
	int64		queries_wiped_out;	/* the number of queries removed from hashtable  */
	int64		plans_wiped_out;	/* the number of plans removed from hashtable  */
	int64		dealloc_time_ms;	/* time spent in deallocating items */
	int64		gc_time_ms;			/* time spent in garbage collection texts */
} pgqpGlobalStats;

/*
 * For mark stored texts which typr it is
 */
typedef enum pgqpTextsKind
{
	PGQP_SQLTEXT = 0,	/* Stored item in query text */
	PGQP_SQLPLAN = 1,	/* Stored item is example execution plan */
	PGQP_GENSQLPLAN = 2 /* Stored item is normalized execution plan */
} pgqpTextsKind;

/*
 * Type of stored value
 */
typedef enum pgqpTextsEnc
{
	PGQP_PLAINTEXT = 0,   /* Stored item as text */
	PGQP_PGLZ = 1		  /* pg_compress-ed*/
} pgqpTextsEnc;

/*
 * Key fo store texts
 */
typedef struct pgqpTextStorageKey
{
	pgqpTextsKind	item_mark;		/* Which type of item is it */
	int64			item_id_low;	/* Unique (among all pgqpTextsKind)
									   identifier for stored item */
	int64			item_id_high;	/* For store query plans we need 2 ids */
} pgqpTextStorageKey;

/*
 * Entry for text storage - here we store query texts and explain plans
 */
typedef struct pgqpTextStorageEntry
{
	pgqpTextStorageKey	text_key;		/* hash key of entry */
	int64				text_offset;	/* text offset in shared memory area */
	size_t				text_len;		/* text len, including the last 0 */
	size_t              origin_text_len; /* origin text length */;
	pgqpTextsEnc		text_encoding;	/* text encoding*/
	int					usage_count;	/* the number of references to item*/
} pgqpTextStorageEntry;

/*
 * Statistics per statement
 */
typedef struct pgqpEntry
{
	pgqpQueryHashKey 		key;			/* hash key of entry - MUST BE FIRST */
	Counters				counters;		/* the statistics for this query */
	pgqpTextStorageEntry*	query_text;     /* Stored query text */
	int64					generation;		/* initialize from pgqpGlobalStats each time
											   query added, so newer be the same even if item was deallocated*/
	int64					plans_count;	/* the number of stored plans for
											   entry in plan hash table*/
	slock_t				mutex;				/* protects the counters only */
} pgqpEntry;

/*
 * Statistics per execution plan
 */
typedef struct pgqpPlanEntry
{
	pgqpPlanHashKey     	key;            /* hash key of entry - MUST BE FIRST */
	Counters            	counters;       /* the statistics for this query */
	PlanCounters			plan_counters;	/* Specific for query plan statistic*/
	pgqpTextStorageEntry*   query_text;     /* Stored example query plan */
	pgqpTextStorageEntry*  	example_plan; 	/* Stored example plan item */
	pgqpTextStorageEntry*   gen_plan;   	/* Stored normalized plan item */
	int64               	generation;     /* initialize from pgqpGlobalStats each time
											   query added, so newer be the same even if item was deallocated*/
	slock_t             	mutex;          /* protects the counters only */
} pgqpPlanEntry;


/*
 * Global shared state
 */
typedef struct pgqpSharedState
{
	LWLock	   *lock;			/* protects hashtable search/modification */
	LWLock	   *memory_lock;	/* protects operations with shared memory */
	double		cur_median_usage;	/* current median usage in hashtable */
	slock_t		mutex;			/* protects following fields only: */
	int64		gc_count;		/* query file garbage collection cycle count */
	int64       generation;     /* increases with each qeury added to table */
	int64		storage_offset; /* current offset in storage */
	pgqpGlobalStats stats;		/* global statistics for pgqp */
} pgqpSharedState;

/* Global variables */

/* Current nesting depth of ExecutorRun+ProcessUtility calls */
extern int exec_nested_level;

/* Current nesting depth of planner calls */
extern int plan_nested_level;

/* Links to shared memory state */
extern pgqpSharedState *pgqp;
extern HTAB *pgqp_queries;
extern HTAB *pgqp_plans;
extern HTAB *pgqp_texts;
extern char *pgqp_storage;

/* Macros to work with adresses in storage */
#define SHMEM_TEXT_PTR(offset) (char*) pgqp_storage + offset

/*---- GUC variables ----*/

typedef enum
{
	PGQP_TRACK_NONE,			/* track no statements */
	PGQP_TRACK_TOP,				/* only top level statements */
	PGQP_TRACK_ALL				/* all statements, including nested ones */
}			PGQPTrackLevel;

extern const struct config_enum_entry track_options[];

extern const struct config_enum_entry plan_formats[];

extern const struct config_enum_entry text_encodings[];

extern const int MEAN_PLANS_PER_QUERY; /* how many plans per query we are going
										  to store */

extern int	pgqp_max;				/* max # statements to track */
extern int  pgqp_max_plans;			/* max # execution plans to track */
extern int 	pgqp_storage_memory; 	/* memory used to store plan and query texts */
extern int  pgqp_max_query_len;		/* maximum query text length */
extern int	pgqp_max_plan_len;	    /* maximum execution plan text length */
extern int	pgqp_track;				/* tracking level */
extern bool pgqp_track_utility; 	/* whether to track utility commands */
extern bool pgqp_track_planning;	/* whether to track planning duration */
extern bool pgqp_track_plans;		/* track execution plans or not */
extern bool pgqp_collapse_plans;	/* collapse plans differ in constants in one */
extern int  pgqp_encoding;			/* compress stored texts */
extern int  example_plan_format;	/* Plan representation style */
extern bool example_log_verbose;	/* Set VERBOSE for EXPLAIN on logging */
extern bool example_log_triggers;	/* Log trigger trace in EXPLAIN */


#define pgqp_enabled(level) \
	(!IsParallelWorker() && \
	(pgqp_track == PGQP_TRACK_ALL || \
	(pgqp_track == PGQP_TRACK_TOP && (level) == 0)))

#define record_gc_qtexts() \
	do { \
		volatile pgqpSharedState *s = (volatile pgqpSharedState *) pgqp; \
		SpinLockAcquire(&s->mutex); \
		s->gc_count++; \
		SpinLockRelease(&s->mutex); \
	} while(0)

#define inc_generation() \
	do { \
		volatile pgqpSharedState *s = (volatile pgqpSharedState *) pgqp; \
		SpinLockAcquire(&s->mutex); \
		s->generation++; \
		SpinLockRelease(&s->mutex); \
	} while(0)

#endif
