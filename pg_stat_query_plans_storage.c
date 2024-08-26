#include "postgres.h"

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
#include "common/pg_lzcompress.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "pg_stat_query_plans_common.h"
#include "pg_stat_query_plans_parser.h"
#include "pg_stat_query_plans_storage.h"

static int	lock_iteration_count = 10;
static int	lock_backoff_timeout = 100;

static void pgqp_update_counters(volatile Counters *counters,
                                 pgqpStoreKind kind, double total_time,
                                 uint64 rows, const BufferUsage *bufusage,
#if PG_VERSION_NUM >= 130000
                                 const WalUsage *walusage,
#else
                                 const void *walusage,
#endif
                                 const struct JitInstrumentation *jitusage);

static void pgqp_remove_text(pgqpTextStorageEntry *entry);

/*
 * Decode stored text
 * if no encoding configured, return text address in the shared storage area
 * otherwise allocate memory, decompress data and return address of decomressed
 * text. Caller should free memory after usage.
 */
char *get_decoded_text(volatile pgqpTextStorageEntry *s) {
  char *uncompressed_dst;
  char *fail_dec_str = "failed to decompress";
  int dlen;

  Assert(s);

  if (s->text_encoding == PGQP_PLAINTEXT)
    return SHMEM_TEXT_PTR(s->text_offset);
  else {
    uncompressed_dst = palloc(s->origin_text_len);
    dlen = pglz_decompress(SHMEM_TEXT_PTR(s->text_offset), s->text_len,
                           uncompressed_dst, s->origin_text_len
#if PG_VERSION_NUM >= 120000
                           ,
                           true
#endif
    );
    if (dlen < 0) {
      pfree(uncompressed_dst);
      uncompressed_dst = palloc(strlen(fail_dec_str) + 1);
      strcpy(uncompressed_dst, fail_dec_str);
    }
    return uncompressed_dst;
  }
}

/*
 * Store texts in storage
 *
 * Caller could call multiple method multiple times
 * So we expect here that:
 * - storage have free space for all data
 * - caller holding exclusive memory lock so no-one could insert data
 * between calls
 */

pgqpTextStorageEntry *pgqp_store_text(const char *data, pgqpTextsKind kind,
                                      uint64 id_low, uint64 id_high) {
  bool found;
  pgqpTextStorageKey key;
  pgqpTextStorageEntry *entry;

  Assert(data);

  if (!pgqp || !pgqp_texts || !pgqp_storage)
    return NULL;

  /* memset() is required when pgqpTextStorageKey is without padding only */
  memset(&key, 0, sizeof(pgqpTextStorageKey));

  key.item_mark = kind;
  key.item_id_low = id_low;
  key.item_id_high = id_high;
  if (id_low == 0) {
    // re-calculate if identifier is zero
    key.item_id_low = hash_any_extended((const unsigned char *)data,
                                        strlen(data), ID_SEEDVALUE);
  }

  entry =
      (pgqpTextStorageEntry *)hash_search(pgqp_texts, &key, HASH_ENTER, &found);

  if (!found) {
    /* the new key - should store new offset value */
    int max_size;
    volatile pgqpSharedState *s = (volatile pgqpSharedState *)pgqp;
    pgqpTextsEnc needed_encoding = pgqp_encoding;

    if (kind == PGQP_SQLTEXT)
      max_size = pgqp_max_query_len;
    else
      max_size = pgqp_max_plan_len;

    /* store offset to string */
    entry->text_offset = s->storage_offset;
    if (needed_encoding == PGQP_PGLZ) {
      char *compressed_dst;
      int len;
      entry->origin_text_len = strlen(data) + 1;
      compressed_dst = palloc(entry->origin_text_len);
      len = pglz_compress(data, entry->origin_text_len, compressed_dst,
                          PGLZ_strategy_default);
      if (len > max_size) {
        // too big to store text - should be truncated
        needed_encoding = PGQP_PLAINTEXT;
      } else if (len > 0) {
        entry->text_len = len;
        Assert(s->storage_offset + entry->text_len < pgqp_storage_memory);
        memcpy(SHMEM_TEXT_PTR(s->storage_offset), compressed_dst, len);
        entry->text_encoding = PGQP_PGLZ;
      } else {
        // most probably data is not compressable - so store them in plain text
        needed_encoding = PGQP_PLAINTEXT;
      }
      pfree(compressed_dst);
    }
    if (needed_encoding == PGQP_PLAINTEXT) {
      if (strlen(data) + 1 <= max_size) {
        /* store text len */
        entry->text_len = strlen(data) + 1;
        Assert(s->storage_offset + entry->text_len < pgqp_storage_memory);
        /* copy data to storage*/
        strcpy(SHMEM_TEXT_PTR(s->storage_offset), data);
      } else {
        char *additional_comment;
        int comment_len =
            asprintf(&additional_comment, "<deleted tail %li symbols>",
                     strlen(data) + 1 - max_size);
        Assert(comment_len > 0);
        /* store text len */
        entry->text_len = max_size + comment_len + 1;
        Assert(s->storage_offset + entry->text_len < pgqp_storage_memory);
        /* copy data to storage*/
        strncpy(SHMEM_TEXT_PTR(s->storage_offset), data, max_size);
        strncpy(SHMEM_TEXT_PTR(s->storage_offset + max_size),
                additional_comment, comment_len + 1);
        free(additional_comment);
      }
      entry->origin_text_len = entry->text_len;
      entry->text_encoding = PGQP_PLAINTEXT;
    }
    /* increase offset */
    s->storage_offset += entry->text_len;
    /* update statistics */
    if (kind == PGQP_SQLTEXT) {
      s->stats.compressed_queries_size += entry->text_len;
      s->stats.queries_size += entry->origin_text_len;
    } else {
      s->stats.compressed_plans_size += entry->text_len;
      s->stats.plans_size += entry->origin_text_len;
    }
    entry->usage_count = 1;
  } else {
    /* increase the number of pointers to data */
    entry->usage_count++;
  }

  return entry;
}

void pgqp_remove_text(pgqpTextStorageEntry *entry) {
  /* Safety check... */
  if (!pgqp || !pgqp_texts || !pgqp_storage)
    return;

  if (entry) {
    entry->usage_count--;
    Assert(entry->usage_count >= 0);
    if (entry->usage_count == 0) {
      volatile pgqpSharedState *s = (volatile pgqpSharedState *)pgqp;
      /* mark string as empty */
      strncpy(SHMEM_TEXT_PTR(entry->text_offset), "", 1);
      /* remove item from hash table */
      hash_search(pgqp_texts, &entry->text_key, HASH_REMOVE, NULL);
      /* update statistics */
      if (entry->text_key.item_mark == PGQP_SQLTEXT) {
        s->stats.queries_size -= entry->origin_text_len;
        s->stats.compressed_queries_size -= entry->text_len;
      } else {
        s->stats.plans_size -= entry->origin_text_len;
        s->stats.compressed_plans_size -= entry->text_len;
      }
      entry->text_len = 1;
    }
  }
};

/*
 * Update counters, method is not thread-safe, so lock should be taken before
 * call update
 */
static void pgqp_update_counters(volatile Counters *counters,
                                 pgqpStoreKind kind, double total_time,
                                 uint64 rows, const BufferUsage *bufusage,
#if PG_VERSION_NUM >= 130000
                                 const WalUsage *walusage,
#else
                                 const void *walusage,
#endif
                                 const struct JitInstrumentation *jitusage) {
  Assert(kind > PGQP_INVALID && kind < PGQP_NUMKIND);
  if (IS_STICKY(counters))
    counters->usage = USAGE_INIT;

  counters->calls[kind] += 1;
  counters->total_time[kind] += total_time;

  if (counters->calls[kind] == 1) {
    counters->min_time[kind] = total_time;
    counters->max_time[kind] = total_time;
    counters->mean_time[kind] = total_time;
  } else {
    /*
     * Welford's method for accurately computing variance. See
     * <http://www.johndcook.com/blog/standard_deviation/>
     */
    double old_mean = counters->mean_time[kind];

    counters->mean_time[kind] +=
        (total_time - old_mean) / counters->calls[kind];
    counters->sum_var_time[kind] +=
        (total_time - old_mean) * (total_time - counters->mean_time[kind]);

    /* calculate min and max time */
    if (counters->min_time[kind] > total_time)
      counters->min_time[kind] = total_time;
    if (counters->max_time[kind] < total_time)
      counters->max_time[kind] = total_time;
  }
  counters->rows += rows;
  counters->shared_blks_hit += bufusage->shared_blks_hit;
  counters->shared_blks_read += bufusage->shared_blks_read;
  counters->shared_blks_dirtied += bufusage->shared_blks_dirtied;
  counters->shared_blks_written += bufusage->shared_blks_written;
  counters->local_blks_hit += bufusage->local_blks_hit;
  counters->local_blks_read += bufusage->local_blks_read;
  counters->local_blks_dirtied += bufusage->local_blks_dirtied;
  counters->local_blks_written += bufusage->local_blks_written;
  counters->temp_blks_read += bufusage->temp_blks_read;
  counters->temp_blks_written += bufusage->temp_blks_written;
  counters->blk_read_time += INSTR_TIME_GET_MILLISEC(bufusage->blk_read_time);
  counters->blk_write_time += INSTR_TIME_GET_MILLISEC(bufusage->blk_write_time);
#if PG_VERSION_NUM >= 150000
  counters->temp_blk_read_time +=
      INSTR_TIME_GET_MILLISEC(bufusage->temp_blk_read_time);
  counters->temp_blk_write_time +=
      INSTR_TIME_GET_MILLISEC(bufusage->temp_blk_write_time);
#endif
  counters->usage += USAGE_EXEC(total_time);
#if PG_VERSION_NUM >= 130000
  counters->wal_records += walusage->wal_records;
  counters->wal_fpi += walusage->wal_fpi;
  counters->wal_bytes += walusage->wal_bytes;
#endif
  if (jitusage) {
    counters->jit_functions += jitusage->created_functions;
    counters->jit_generation_time +=
        INSTR_TIME_GET_MILLISEC(jitusage->generation_counter);

    if (INSTR_TIME_GET_MILLISEC(jitusage->inlining_counter))
      counters->jit_inlining_count++;
    counters->jit_inlining_time +=
        INSTR_TIME_GET_MILLISEC(jitusage->inlining_counter);

    if (INSTR_TIME_GET_MILLISEC(jitusage->optimization_counter))
      counters->jit_optimization_count++;
    counters->jit_optimization_time +=
        INSTR_TIME_GET_MILLISEC(jitusage->optimization_counter);

    if (INSTR_TIME_GET_MILLISEC(jitusage->emission_counter))
      counters->jit_emission_count++;
    counters->jit_emission_time +=
        INSTR_TIME_GET_MILLISEC(jitusage->emission_counter);
  }
}

#if PG_VERSION_NUM < 140000
/*
 * Given an arbitrarily long query string, produce a hash for the purposes of
 * identifying the query, without normalizing constants.  Used when hashing
 * utility statements.
 */
static uint64 pgqp_hash_string(const char *str, int len) {
  return DatumGetUInt64(hash_any_extended((const unsigned char *)str, len, 0));
}
#endif

/*
 * Store some statistics for a statement.
 *
 * If jstate is not NULL then we're trying to create an entry for which
 * we have no statistics as yet; we just want to record the normalized
 * query string.  total_time, rows, bufusage and walusage are ignored in this
 * case.
 *
 * If kind is PGQP_PLAN or PGQP_EXEC, its value is used as the array position
 * for the arrays in the Counters field.
 */
void pgqp_store(const char *query, StringInfo execution_plan, uint64 queryId,
                QueryDesc *qd, int query_location, int query_len,
                pgqpStoreKind kind, double total_time, uint64 rows,
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
) {
  pgqpQueryHashKey key;
  pgqpPlanHashKey plan_key;
  pgqpEntry *entry = NULL;
  pgqpPlanEntry *plan_entry = NULL;
  int64 generation;

  Assert(query != NULL);
  Assert(kind == PGQP_PLAN || kind == PGQP_EXEC || jstate != NULL);

  /* Safety check... */
  if (!pgqp || !pgqp_queries || !pgqp_plans || !pgqp_texts) {
    return;
  }

#if PG_VERSION_NUM >= 140000
  /*
   * Nothing to do if compute_query_id isn't enabled and no other module
   * computed a query identifier.
   */
  if (queryId == UINT64CONST(0)) {
    return;
  }

  /*
   * Confine our attention to the relevant part of the string, if the query
   * is a portion of a multi-statement source string, and update query
   * location and length if needed.
   */
  query = CleanQuerytext(query, &query_location, &query_len);
#else
  if (query_location >= 0) {
    Assert(query_location <= strlen(query));
    query += query_location;
    /* Length of 0 (or -1) means "rest of string" */
    if (query_len <= 0)
      query_len = strlen(query);
    else
      Assert(query_len <= strlen(query));
  } else {
    /* If query location is unknown, distrust query_len as well */
    query_location = 0;
    query_len = strlen(query);
  }

  /*
   * Discard leading and trailing whitespace, too.  Use scanner_isspace()
   * not libc's isspace(), because we want to match the lexer's behavior.
   */
  while (query_len > 0 && scanner_isspace(query[0]))
    query++, query_location++, query_len--;
  while (query_len > 0 && scanner_isspace(query[query_len - 1]))
    query_len--;

  /*
   * For utility statements, we just hash the query string to get an ID.
   */
  if (queryId == UINT64CONST(0)) {
    queryId = pgqp_hash_string(query, query_len);

    /*
     * If we are unlucky enough to get a hash of zero(invalid), use
     * queryID as 2 instead, queryID 1 is already in use for normal
     * statements.
     */
    if (queryId == UINT64CONST(0))
      queryId = UINT64CONST(2);
  }
#endif

  /* memset() is required when pgqpQueryHashKey is without padding only */
  memset(&key, 0, sizeof(pgqpQueryHashKey));

  key.userid = GetUserId();
  key.dbid = MyDatabaseId;
  key.queryid = queryId;
  key.toplevel = (exec_nested_level == 0);

  /* Lookup the hash table entry with shared lock. */
  LWLockAcquire(pgqp->lock, LW_SHARED);

  entry = (pgqpEntry *)hash_search(pgqp_queries, &key, HASH_FIND, NULL);

  /* Create new entry, if not present */
  if (!entry) {
    char *norm_query;
    int need_free = 0;
    /*
     * Create a new, normalized query string if caller asked.  We don't
     * need to hold the lock while doing this work.  (Note: in any case,
     * it's possible that someone else creates a duplicate hashtable entry
     * in the interval where we don't hold the lock below.  That case is
     * handled by entry_alloc.)
     */
    if (jstate) {
      LWLockRelease(pgqp->lock);
      norm_query = gen_normquery(jstate, query, query_location, &query_len);
      need_free = 1;
      LWLockAcquire(pgqp->lock, LW_SHARED);
    } else {
      norm_query = (char*)query;
    }
    /* Need exclusive lock to make a new hashtable entry - promote */
    LWLockRelease(pgqp->lock);
    if (lock_iteration_count == 0)
        LWLockAcquire(pgqp->lock, LW_EXCLUSIVE);
    else
	for (int i = 0; i < lock_iteration_count; i++)
	{
		if (LWLockConditionalAcquire(pgqp->lock, LW_EXCLUSIVE))
			break;
		if (i > lock_iteration_count / 2)
			pg_usleep(i * lock_backoff_timeout);
		if (i == lock_iteration_count - 1)
		{
			elog(DEBUG1, "could not enter query information to pgqp_ya");
			return;
		}
	}

    /* OK to create a new hashtable entry */
    {
      volatile pgqpSharedState *s = (volatile pgqpSharedState *)pgqp;
      SpinLockAcquire(&s->mutex);
      generation = s->generation;
      SpinLockRelease(&s->mutex);
    }
    entry = pgqp_query_alloc(&key, jstate != NULL, generation);
    inc_generation();
    entry->query_text = pgqp_store_text(norm_query, PGQP_SQLTEXT, queryId, 0);
    /* Not need exclusive lock for a while - switch to shared  */
    LWLockRelease(pgqp->lock);
    LWLockAcquire(pgqp->lock, LW_SHARED);

    /* We postpone this clean-up until we're out of the lock */
    if (need_free && norm_query)
        pfree(norm_query);

  }

  if (execution_plan) {
    int64 planId;
    Assert(qd);

    planId = hash_any_extended((const unsigned char *)execution_plan->data,
                               execution_plan->len, ID_SEEDVALUE);
    memset(&plan_key, 0, sizeof(pgqpPlanHashKey));

    plan_key.userid = GetUserId();
    plan_key.dbid = MyDatabaseId;
    plan_key.queryid = queryId;
    plan_key.toplevel = (exec_nested_level == 0);
    plan_key.planid = planId;

    plan_entry =
        (pgqpPlanEntry *)hash_search(pgqp_plans, &plan_key, HASH_FIND, NULL);

    /* Create new entry, if not present */
    if (!plan_entry) {
      ExplainState *es;
      /* Need exclusive lock to make a new hashtable entry - promote */
      LWLockRelease(pgqp->lock);
      if (lock_iteration_count == 0)
          LWLockAcquire(pgqp->lock, LW_EXCLUSIVE);
      else
		for (int i = 0; i < lock_iteration_count; i++)
		{
			if (LWLockConditionalAcquire(pgqp->lock, LW_EXCLUSIVE))
				break;
			if (i > lock_iteration_count / 2)
				pg_usleep(i * lock_backoff_timeout);
			if (i == lock_iteration_count - 1)
			{
				elog(DEBUG1, "could not enter query information to pgsk");
				return;
			}
		}
      /* OK to create a new hashtable entry */
      {
        volatile pgqpSharedState *s = (volatile pgqpSharedState *)pgqp;
        SpinLockAcquire(&s->mutex);
        generation = s->generation;
        SpinLockRelease(&s->mutex);
      }
      plan_entry = pgqp_plan_alloc(&plan_key, jstate != NULL, generation);
      inc_generation();
      /* Store original query text */
      plan_entry->query_text =
          pgqp_store_text(query, PGQP_SQLTEXT, planId, queryId);
      /* Store normalized plan */
      plan_entry->gen_plan = pgqp_store_text(execution_plan->data,
                                             PGQP_GENSQLPLAN, planId, queryId);
      /*
       * Now make explain plan again - with user settings to show it in
       * pg_stat_query_plans_plan_ya
       */
      es = NewExplainState();
      es->verbose = example_log_verbose;
      es->format = example_plan_format;
      ExplainBeginOutput(es);
      ExplainPrintPlan(es, qd);
      if (example_log_triggers)
        ExplainPrintTriggers(es, qd);
      ExplainEndOutput(es);
      /* Store plan text */
      plan_entry->example_plan =
          pgqp_store_text(es->str->data, PGQP_SQLPLAN, planId, queryId);
      /* Increase plans counter for query */
      /* Query entry could be wiped out here so we should search it again */
      entry = (pgqpEntry *)hash_search(pgqp_queries, &key, HASH_FIND, NULL);
      if (entry) {
        entry->plans_count++;
      }
    }
  }

  /* Increment the counts, except when jstate is not NULL */
  if (!jstate) {
    if (entry) {
      /*
       * Grab the spinlock while updating the counters (see comment about
       * locking rules at the head of the file)
       */
      volatile pgqpEntry *e = (volatile pgqpEntry *)entry;

      SpinLockAcquire(&e->mutex);

      pgqp_update_counters(&e->counters, kind, total_time, rows, bufusage,
                           walusage, jitusage);

      SpinLockRelease(&e->mutex);
    }

    if (plan_entry) {
      volatile pgqpPlanEntry *e_plan = (volatile pgqpPlanEntry *)plan_entry;
      SpinLockAcquire(&e_plan->mutex);
      pgqp_update_counters(&e_plan->counters, kind, total_time, rows, bufusage,
                           walusage, jitusage);
      if (qd) {
        plan_entry->plan_counters.startup_cost =
            qd->plannedstmt->planTree->startup_cost;
        plan_entry->plan_counters.total_cost =
            qd->plannedstmt->planTree->total_cost;
        plan_entry->plan_counters.plan_rows =
            qd->plannedstmt->planTree->plan_rows;
        plan_entry->plan_counters.plan_width =
            qd->plannedstmt->planTree->plan_width;
      }
      SpinLockRelease(&e_plan->mutex);
    }
  }

  LWLockRelease(pgqp->lock);
}

/*
 * All methods here started with pgqp_ prefix are not thread-safe.
 * Caller must hold an exclusive lock on pgqp->lock.
 */

/*
 * Allocate a new hashtable entry.
 *
 * If "sticky" is true, make the new entry artificially sticky so that it will
 * probably still be there when the query finishes execution.  We do this by
 * giving it a median usage value rather than the normal value.  (Strictly
 * speaking, query strings are normalized on a best effort basis, though it
 * would be difficult to demonstrate this even under artificial conditions.)
 *
 * Note: despite needing exclusive lock, it's not an error for the target
 * entry to already exist.  This is because pgqp_store releases and
 * reacquires lock after failing to find a match; so someone else could
 * have made the entry while we waited to get exclusive lock.
 */
pgqpEntry *pgqp_query_alloc(pgqpQueryHashKey *key, bool sticky,
                            int64 generation) {
  pgqpEntry *entry;
  bool found;

  /* Make space if needed */
  pgqp_dealloc();

  /* Find or create an entry with desired hash code */
  entry = (pgqpEntry *)hash_search(pgqp_queries, key, HASH_ENTER, &found);

  if (!found) {
    /* New entry, initialize it */
    /* reset the statistics */
    memset(&entry->counters, 0, sizeof(Counters));
    /* set the appropriate initial usage count */
    entry->counters.usage = sticky ? pgqp->cur_median_usage : USAGE_INIT;
    /* re-initialize the mutex each time ... we assume no one using it */
    SpinLockInit(&entry->mutex);
    /* set generation */
    entry->generation = generation;
    /* there is no stored plans */
    entry->plans_count = 0;
  }

  return entry;
}

/*
 * Allocate a new hashtable entry. Similar to pgqp_query_alloc
 */
pgqpPlanEntry *pgqp_plan_alloc(pgqpPlanHashKey *key, bool sticky,
                               int64 generation) {
  pgqpPlanEntry *entry;
  bool found;

  /* Make space if needed */
  pgqp_dealloc();

  /* Find or create an entry with desired hash code */
  entry = (pgqpPlanEntry *)hash_search(pgqp_plans, key, HASH_ENTER, &found);

  if (!found) {
    /* New entry, initialize it */
    /* reset the statistics */
    memset(&entry->counters, 0, sizeof(Counters));
    memset(&entry->plan_counters, 0, sizeof(PlanCounters));
    /* set the appropriate initial usage count */
    entry->counters.usage = sticky ? pgqp->cur_median_usage : USAGE_INIT;
    /* re-initialize the mutex each time ... we assume no one using it */
    SpinLockInit(&entry->mutex);
    /* set generation */
    entry->generation = generation;
  }

  return entry;
}

/*
 * qsort sql entry comparator for sorting into increasing usage order
 */
static int entry_dealloc_cmp(const void *lhs, const void *rhs) {
  double l_usage = (*(pgqpEntry *const *)lhs)->counters.usage;
  double r_usage = (*(pgqpEntry *const *)rhs)->counters.usage;

  if (l_usage < r_usage)
    return -1;
  else if (l_usage > r_usage)
    return +1;
  else
    return 0;
}

/*
 * qsort plan entry comparator for sorting into increasing usage order
 */
static int plan_entry_dealloc_cmp(const void *lhs, const void *rhs) {
  double l_usage = (*(pgqpPlanEntry *const *)lhs)->counters.usage;
  double r_usage = (*(pgqpPlanEntry *const *)rhs)->counters.usage;

  if (l_usage < r_usage)
    return -1;
  else if (l_usage > r_usage)
    return +1;
  else
    return 0;
}

/*
 * qsort comparator for sorting into increasing offset order
 */
static int entry_gc_cmp(const void *lhs, const void *rhs) {
  int64 l_offset = (*(pgqpTextStorageEntry *const *)lhs)->text_offset;
  int64 r_offset = (*(pgqpTextStorageEntry *const *)rhs)->text_offset;

  if (l_offset < r_offset)
    return -1;
  else if (l_offset > r_offset)
    return +1;
  else
    return 0;
}

/*
 * Compact data in a storage and change offsets for texts
 */
void pgqp_gc_storage(void) {
  HASH_SEQ_STATUS hash_seq;
  pgqpTextStorageEntry **entries;
  pgqpTextStorageEntry *entry;
  int i;
  int items_count;
  int64 storage_offset;
  int64 current_offset;
  instr_time start;
  instr_time duration;
  volatile pgqpSharedState *s = (volatile pgqpSharedState *)pgqp;

  /* copy variables while holding lock */
  SpinLockAcquire(&s->mutex);
  storage_offset = s->storage_offset;
  SpinLockRelease(&s->mutex);

  /* check if we really need to perform gc */
  if (pgqp_storage_memory - storage_offset >
      pgqp_storage_memory / 100 * FREE_PERCENT) {
    return;
  }

  /* no-one coud change memory structures while we performing gc */
  SpinLockAcquire(&s->mutex);

  INSTR_TIME_SET_CURRENT(start);

  entries =
      palloc(hash_get_num_entries(pgqp_texts) * sizeof(pgqpTextStorageEntry *));
  /*
   * Sort entries by offset and deallocate not used entries.
   * Copy string data in a shared memory storage while deallocating unused
   * items.
   */
  i = 0;
  hash_seq_init(&hash_seq, pgqp_texts);
  while ((entry = hash_seq_search(&hash_seq)) != NULL) {
    if (entry->usage_count == 0) {
      hash_search(pgqp_texts, &entry->text_key, HASH_REMOVE, NULL);
    } else {
      entries[i++] = entry;
    }
  }

  items_count = i;
  /* Sort into increasing order by current_offset */
  qsort(entries, items_count, sizeof(pgqpTextStorageEntry *), entry_gc_cmp);

  /* now shift data */
  current_offset = 0;
  for (i = 0; i < items_count; i++) {
    Assert(entries[i]->text_offset >= current_offset);
    if (entries[i]->text_offset > current_offset) {
      memmove(SHMEM_TEXT_PTR(current_offset),
              SHMEM_TEXT_PTR(entries[i]->text_offset), entries[i]->text_len);
      entries[i]->text_offset = current_offset;
    }
    current_offset += entries[i]->text_len;
  }
  Assert(current_offset <= storage_offset);
  /* set storage_offset to the last position */
  s->storage_offset = current_offset;
  s->gc_count++;

  pfree(entries);

  INSTR_TIME_SET_CURRENT(duration);
  INSTR_TIME_SUBTRACT(duration, start);
  s->stats.gc_time_ms += INSTR_TIME_GET_MILLISEC(duration);
  SpinLockRelease(&s->mutex);
};

/*
 * checking if dealloc of items needed, flag already_started change boundaries
 * for decision - in a case of running deallocation we should dealloc additional
 * items to have reserve
 */
bool need_gc(bool already_started, int64 queries_size, int64 plans_size) {
  int64 free_entries = 1;
  int64 free_entries_plan = 1;
  int64 smemory = pgqp_max_query_len + pgqp_max_plan_len;

  if (already_started) {
    free_entries = Max(1, pgqp_max * FREE_PERCENT / 100);
    free_entries_plan = Max(1, pgqp_max_plans * FREE_PERCENT / 100);
    smemory = Max(smemory, pgqp_storage_memory / 100 * FREE_PERCENT);
  }

  /* check if pgqp_queries need be cleared */
  if (hash_get_num_entries(pgqp_queries) >= pgqp_max - free_entries)
    return true;

  /* check if plans need be cleared */
  if (hash_get_num_entries(pgqp_plans) >= pgqp_max_plans - free_entries_plan)
    return true;

  /* check if storage need be cleared*/
  if (pgqp_storage_memory <= queries_size + plans_size + smemory)
    return true;

  return false;
}

/*
 * checking if dealloc query texts is needed, method could be called only while
 * we've already started items deallocation
 */
static bool need_gc_stat(int64 queries_size) {
  int64 free_entries = Max(1, pgqp_max * FREE_PERCENT / 100);
  int64 smemory = pgqp_max_query_len + pgqp_max_plan_len;

  smemory = Max(smemory, pgqp_storage_memory / 100 * FREE_PERCENT);

  if (hash_get_num_entries(pgqp_queries) >= pgqp_max - free_entries)
    return true;

  /* check if clearing stored query texts could help free storage memory */
  if (pgqp_storage_memory <= queries_size + smemory)
    return true;

  return false;
}

/*
 * Deallocate least-used query entries.
 */
void pgqp_dealloc(void) {
  HASH_SEQ_STATUS hash_seq;
  pgqpPlanEntry **entries;
  pgqpPlanEntry *entry;
  pgqpEntry *query_entry;
  pgqpQueryHashKey key;
  int item_count;
  int i;
  instr_time start;
  instr_time duration;

  volatile pgqpSharedState *s = (volatile pgqpSharedState *)pgqp;

  /* Check if dealloc needed */

  if (!need_gc(false, s->stats.queries_size, s->stats.plans_size))
    return;

  /*
   * Sort entries by usage and deallocate while not satisfied need_gc criteria.
   * While we're scanning the table, apply the decay factor to the usage
   * values.
   *
   * Note that the mean query length is almost immediately obsolete, since
   * we compute it before not after discarding the least-used entries.
   * Hopefully, that doesn't affect the mean too much; it doesn't seem worth
   * making two passes to get a more current result.  Likewise, the new
   * cur_median_usage includes the entries we're about to zap.
   */

  INSTR_TIME_SET_CURRENT(start);
  entries = palloc(hash_get_num_entries(pgqp_plans) * sizeof(pgqpPlanEntry *));

  /* Create array for sotring and decay items */
  i = 0;
  hash_seq_init(&hash_seq, pgqp_plans);
  while ((entry = hash_seq_search(&hash_seq)) != NULL) {
    Counters *c = &entry->counters;
    entries[i++] = entry;
    /* "Sticky" entries get a different usage decay rate. */
    if (IS_STICKY(c))
      entry->counters.usage *= STICKY_DECREASE_FACTOR;
    else
      entry->counters.usage *= USAGE_DECREASE_FACTOR;
  }

  /* Sort into increasing order by usage */
  qsort(entries, i, sizeof(pgqpPlanEntry *), plan_entry_dealloc_cmp);

  /* Record the (approximate) median usage */
  if (i > 0)
    pgqp->cur_median_usage = entries[i / 2]->counters.usage;

  /*  No-one could re-use memory while we free all structures */
  {
    /* remove items */
    SpinLockAcquire(&s->mutex);
    item_count = i;
    for (i = 0; i < item_count; i++) {
      /* remove entry from pgqp_plans*/
      entry = hash_search(pgqp_plans, &entries[i]->key, HASH_REMOVE, NULL);
      Assert(entry);
      /* remove link to query text */
      pgqp_remove_text(entry->query_text);
      /* remove link to query plans */
      pgqp_remove_text(entry->example_plan);
      pgqp_remove_text(entry->gen_plan);
      /* increase statistics about query remove */
      s->stats.plans_wiped_out += 1;
      /* remove entry from pgqp_queries */
      memset(&key, 0, sizeof(pgqpQueryHashKey));
      key.userid = entry->key.userid;
      key.dbid = entry->key.dbid;
      key.queryid = entry->key.queryid;
      key.toplevel = entry->key.toplevel;
      query_entry =
          (pgqpEntry *)hash_search(pgqp_queries, &key, HASH_FIND, NULL);
      if (query_entry) {
        query_entry->plans_count--;
        /* if no plans left - we could remove query_entry
         * removing query could lead to store un-normalized query texts
         * (normalized plan store only in post-parse steps),
         * so we do not remove if really not need to.
         * plans_count could be negative if query was removed during
         * plan registration
         */
        if (query_entry->plans_count <= 0 &&
            need_gc_stat(s->stats.queries_size)) {
          query_entry = (pgqpEntry *)hash_search(
              pgqp_queries, &query_entry->key, HASH_REMOVE, NULL);
          Assert(query_entry);
          /* remove link to query text */
          pgqp_remove_text(query_entry->query_text);
          /* increase statistics about query remove */
          s->stats.queries_wiped_out += 1;
        }
      }

      /* Check if we still need to delete items */
      if (!need_gc(true, s->stats.queries_size, s->stats.plans_size))
        break;
    }

    /* if garbage collection is still needed - it means no execution plans left
     * and pgqp_queries contains only items without plans - so make usual gc for
     * queries (usefull if we do not store execution plans as we perform here
     * usual GC)
     */
    if (need_gc_stat(s->stats.queries_size)) {
      pgqpEntry **query_entries;
      query_entries =
          palloc(hash_get_num_entries(pgqp_queries) * sizeof(pgqpEntry *));
      i = 0;
      hash_seq_init(&hash_seq, pgqp_queries);
      while ((query_entry = hash_seq_search(&hash_seq)) != NULL) {
        Counters *c = &query_entry->counters;
        query_entries[i++] = query_entry;
        if (IS_STICKY(c))
          query_entry->counters.usage *= STICKY_DECREASE_FACTOR;
        else
          query_entry->counters.usage *= USAGE_DECREASE_FACTOR;
      }
      /* Sort into increasing order by usage */
      qsort(query_entries, i, sizeof(pgqpEntry *), entry_dealloc_cmp);
      item_count = i;
      for (i = 0; i < item_count; i++) {
        /* remove entry from pgqp_queries*/
        query_entry = hash_search(pgqp_queries, &query_entries[i]->key,
                                  HASH_REMOVE, NULL);
        Assert(query_entry);
        /* remove link to query text */
        pgqp_remove_text(query_entry->query_text);
        /* increase statistics about query remove */
        s->stats.queries_wiped_out += 1;
        /* Check if we still need to delete items */
        if (!need_gc_stat(s->stats.queries_size))
          break;
      }
      if (query_entries)
        pfree(query_entries);
    }
    /* Increment the number of times entries are deallocated */
    s->stats.dealloc += 1;
    SpinLockRelease(&s->mutex);
  }

  if (entries)
    pfree(entries);

  INSTR_TIME_SET_CURRENT(duration);
  INSTR_TIME_SUBTRACT(duration, start);
  SpinLockAcquire(&s->mutex);
  s->stats.dealloc_time_ms += INSTR_TIME_GET_MILLISEC(duration);
  SpinLockRelease(&s->mutex);

  pgqp_gc_storage();
}

/*
 * Release entries corresponding to parameters passed.
 * It's time-consuming operation, all operations will be frozen for clearing
 */
void pgqp_entry_reset(Oid userid, Oid dbid, uint64 queryid) {
  HASH_SEQ_STATUS hash_seq;
  pgqpEntry *entry;
  pgqpPlanEntry *plan_entry;
  long num_entries;
  long num_remove = 0;
  instr_time start;
  instr_time duration;
  volatile pgqpSharedState *s = (volatile pgqpSharedState *)pgqp;

  if (!pgqp || !pgqp_queries || !pgqp_plans || !pgqp_texts)
    ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("pg_stat_query_plans must be loaded via "
                           "shared_preload_libraries")));

  if (lock_iteration_count == 0)
  	LWLockAcquire(pgqp->lock, LW_EXCLUSIVE);
  else
		for (int i = 0; i < lock_iteration_count; i++)
		{
			if (LWLockConditionalAcquire(pgqp->lock, LW_EXCLUSIVE))
				break;
			if (i > lock_iteration_count / 2)
				pg_usleep(i * lock_backoff_timeout);
			if (i == lock_iteration_count - 1)
			{
				elog(WARNING, "could not reset query information of pgqp_ya");
				return;
			}
		}
  SpinLockAcquire(&s->mutex);
  INSTR_TIME_SET_CURRENT(start);
  num_entries = hash_get_num_entries(pgqp_queries);

  /* We should scan all entries and delete corresponding items */
  if (userid != 0 || dbid != 0 || queryid != UINT64CONST(0)) {
    /* Remove queries */
    hash_seq_init(&hash_seq, pgqp_queries);
    while ((entry = hash_seq_search(&hash_seq)) != NULL) {
      if ((!userid || entry->key.userid == userid) &&
          (!dbid || entry->key.dbid == dbid) &&
          (!queryid || entry->key.queryid == queryid)) {
        entry = hash_search(pgqp_queries, &entry->key, HASH_REMOVE, NULL);
        /* remove link to query text */
        pgqp_remove_text(entry->query_text);
        /* increase statistics about query remove */
        s->stats.queries_wiped_out += 1;
        num_remove++;
      }
    }
    /* Remove plans */
    hash_seq_init(&hash_seq, pgqp_plans);
    while ((plan_entry = hash_seq_search(&hash_seq)) != NULL) {
      if ((!userid || plan_entry->key.userid == userid) &&
          (!dbid || plan_entry->key.dbid == dbid) &&
          (!queryid || plan_entry->key.queryid == queryid)) {
        plan_entry =
            hash_search(pgqp_plans, &plan_entry->key, HASH_REMOVE, NULL);
        /* remove link to query text */
        pgqp_remove_text(plan_entry->query_text);
        /* remove link to execution text */
        pgqp_remove_text(plan_entry->example_plan);
        pgqp_remove_text(plan_entry->gen_plan);
        /* increase statistics about plans remove */
        s->stats.plans_wiped_out += 1;
      }
    }
    /* Increment the number of times entries are deallocated */
    s->stats.dealloc += 1;
  } else {
    /* Remove all entries. */
    hash_seq_init(&hash_seq, pgqp_queries);
    while ((entry = hash_seq_search(&hash_seq)) != NULL) {
      entry = hash_search(pgqp_queries, &entry->key, HASH_REMOVE, NULL);
      /* remove link to query text */
      pgqp_remove_text(entry->query_text);
      /* increase statistics about query remove */
      s->stats.queries_wiped_out += 1;
      num_remove++;
    }
    /* Remove all plans */
    hash_seq_init(&hash_seq, pgqp_plans);
    while ((plan_entry = hash_seq_search(&hash_seq)) != NULL) {
      plan_entry = hash_search(pgqp_plans, &plan_entry->key, HASH_REMOVE, NULL);
      /* remove link to query text */
      pgqp_remove_text(plan_entry->query_text);
      /* remove link to execution text */
      pgqp_remove_text(plan_entry->example_plan);
      pgqp_remove_text(plan_entry->gen_plan);
      /* increase statistics about plans remove */
      s->stats.plans_wiped_out += 1;
    }
  }

  /* All entries are removed? */
  if (num_entries <= num_remove) {
    /*
     * Reset global statistics for pg_stat_query_plans since all entries are
     * removed.
     */
    TimestampTz stats_reset = GetCurrentTimestamp();
    s->stats.dealloc = 0;
    s->stats.stats_reset = stats_reset;
    s->stats.queries_wiped_out = 0;
    s->stats.plans_wiped_out = 0;
  }

  INSTR_TIME_SET_CURRENT(duration);
  INSTR_TIME_SUBTRACT(duration, start);
  s->stats.dealloc_time_ms += INSTR_TIME_GET_MILLISEC(duration);
  SpinLockRelease(&s->mutex);
  pgqp_gc_storage();
  LWLockRelease(pgqp->lock);
}
