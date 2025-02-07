#include "pg_stat_query_plans_parser.h"

pgqpEntry *pgqp_query_alloc(pgqpQueryHashKey *key, bool sticky, int64 generation);

pgqpPlanEntry *pgqp_plan_alloc(pgqpPlanHashKey *key, bool sticky, int64 generation);

pgqpTextStorageEntry *pgqp_store_text(const char *data, pgqpTextsKind kind,
									  uint64 id_low,
									  uint64 id_high);

void pgqp_store(const char *query,
				uint64 planId,
				uint64 queryId,
				QueryDesc *qd,
				int query_location, int query_len,
				pgqpStoreKind kind,
				double total_time, uint64 rows,
				const BufferUsage *bufusage,
#if PG_VERSION_NUM >= 130000
				const WalUsage *walusage,
#else
				const void *walusage,
#endif
				const struct JitInstrumentation *jitusage,
#if PG_VERSION_NUM >= 140000
				JumbleState *jstate
#else
				pgqpJumbleState *jstate
#endif
				);

char *get_decoded_text(volatile pgqpTextStorageEntry *s);

void pgqp_entry_reset(Oid userid, Oid dbid, uint64 queryid);

/* methods not used anywhere except unit testing */

bool pgqp_need_gc(bool already_started, int64 queries_size, int64 plans_size);

void pgqp_dealloc(void);

void pgqp_gc_storage(void);
