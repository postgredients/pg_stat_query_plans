#include <stdio.h>
#include <check.h>
#include <unistd.h>
#include "postgres.h"
#include "pg_config.h"
#include "nodes/nodes.h"
#include "utils/queryjumble.h"
#include "lib/stringinfo.h"
#include "executor/instrument.h"
#include "executor/execdesc.h"
#include "storage/lwlock.h"
#include "utils/guc.h"
#include "storage/shmem.h"
#include "storage/ipc.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "storage/pg_shmem.h"
#include "storage/pg_sema.h"
#include "storage/lock.h"
#include "../../pg_stat_query_plans_common.h"
#include "../../pg_stat_query_plans_storage.h"

const char *progname;

static void
pgqp_shmem_request(void)
{
  RequestNamedLWLockTranche("pg_stat_query_plans", 1);
}

static void
pgqp_shmem_init(void)
{
	bool            found;
	HASHCTL     info_queries;
	HASHCTL     info_plans;
	HASHCTL     info_texts;

	/*
	 * Create or attach to the shared memory state, including hash table
	*/

	// RequestAddinShmemSpace(pgqp_memsize());
	pgqp = ShmemInitStruct("pg_stat_query_plans", sizeof(pgqpSharedState), &found);

	pgqp->lock = &(GetNamedLWLockTranche("pg_stat_query_plans"))->lock;

	info_queries.keysize = sizeof(pgqpQueryHashKey);
	info_queries.entrysize = sizeof(pgqpEntry);
	pgqp_queries = ShmemInitHash("pg_stat_query_plans hash",
								 pgqp_max, pgqp_max,
								 &info_queries,
								 HASH_ELEM | HASH_BLOBS);

	pgqp_max_plans = pgqp_max * MEAN_PLANS_PER_QUERY;
	info_plans.keysize = sizeof(pgqpPlanHashKey);
	info_plans.entrysize = sizeof(pgqpPlanEntry);
	pgqp_plans = ShmemInitHash("pg_stat_query_plans hash",
							   pgqp_max_plans, pgqp_max_plans,
							   &info_plans,
							   HASH_ELEM | HASH_BLOBS);

	info_texts.keysize = sizeof(pgqpTextStorageKey);
	info_texts.entrysize = sizeof(pgqpTextStorageEntry);
	pgqp_texts = ShmemInitHash("pg_stat_query_plans texts",
							   pgqp_max_plans + pgqp_max, pgqp_max_plans + pgqp_max,
							   &info_texts,
							   HASH_ELEM | HASH_BLOBS);

	pgqp_storage = ShmemInitStruct("pg_stat_query_plans storage",
								   pgqp_storage_memory,
								   &found);

}

START_TEST(test_pgqp_alloc)
{
	pgqpEntry		*entry = NULL;
	pgqpPlanEntry	*plan_entry = NULL;
	pgqpQueryHashKey        key;
	pgqpPlanHashKey         plan_key;
	key.userid = 1;
	key.dbid = 1;
	key.queryid = 3;
	key.toplevel = true;
	entry = pgqp_query_alloc(&key, false, 1);
	ck_assert(entry);
	ck_assert(entry->counters.usage == USAGE_INIT);
	ck_assert(entry->plans_count == 0);
	ck_assert(entry->generation == 1);
	key.queryid = 4;
	entry = pgqp_query_alloc(&key, true, 2);
	ck_assert(entry);
	ck_assert(entry->counters.usage == pgqp->cur_median_usage);
	ck_assert(entry->plans_count == 0);
	ck_assert(entry->generation == 2);
	entry = pgqp_query_alloc(&key, true, 3);
	ck_assert(entry);
	ck_assert(entry->generation == 2);

	ck_assert(hash_get_num_entries(pgqp_queries) == 2);

	plan_key.userid = 1;
	plan_key.dbid = 1;
	plan_key.queryid = 3;
	plan_key.toplevel = true;
	plan_key.planid = 1;
	plan_entry = pgqp_plan_alloc(&plan_key, false, 3);
	ck_assert(plan_entry);
	ck_assert(plan_entry->counters.usage == USAGE_INIT);
	ck_assert(plan_entry->generation == 3);
	plan_key.queryid = 2;
	plan_entry = pgqp_plan_alloc(&plan_key, true, 4);
	ck_assert(plan_entry);
	ck_assert(plan_entry->counters.usage == pgqp->cur_median_usage);
	ck_assert(plan_entry->generation == 4);
	plan_key.planid = 2;
	plan_entry = pgqp_plan_alloc(&plan_key, true, 5);
	ck_assert(plan_entry);
	ck_assert(plan_entry->generation == 5);
	plan_key.queryid = 3;
	plan_key.planid = 1;
	plan_entry = pgqp_plan_alloc(&plan_key, true, 6);
	ck_assert(plan_entry);
	ck_assert(plan_entry->generation == 3);

	ck_assert(hash_get_num_entries(pgqp_plans) == 3);
}
END_TEST

START_TEST(test_pgqp_text)
{
	pgqpTextStorageEntry*   entry_item;
	char *example_str1 = "1;";
	char *example_str2 = "SELECT 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 ,"
	"14 , 15 , 16 , 17 , 18 , 19 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 ,"
	"30 , 31 , 32 , 33 , 34 , 35 , 36 , 37 , 38 , 39 , 40 , 41 , 42 , 43 , 44 , 45 ,"
	"46 , 47 , 48 , 49 , 50 , 51 , 52 , 53 , 54 , 55 , 56 , 57 , 58 , 59 , 60 , 61 ,"
	"62 , 63 , 64 , 65 , 66 , 67 , 68 , 69 , 70 , 71 , 72 , 73 , 74 , 75 , 76 , 77 ,"
	"78 , 79 , 80 , 81 , 82 , 83 , 84 , 85 , 86 , 87 , 88 , 89 , 90 , 91 , 92 , 93 ,"
	"94 , 95 , 96 , 97 , 98 , 99;";
	char *tmp_str;
	char *deleted_str = "1<deleted tail 2 symbols>";
	char *deleted_str2 = "S<deleted tail 489 symbols>";
	int64 test_offset = 0;
	pgqp_max_query_len = 1 * 1024;
	pgqp_max_plan_len = 1 * 1024;
	pgqp_encoding = PGQP_PGLZ;
	entry_item = pgqp_store_text(example_str1, PGQP_SQLTEXT, 3, 0);
	ck_assert(entry_item);
	ck_assert(entry_item->origin_text_len == strlen(example_str1) + 1);
	ck_assert(entry_item->text_len == strlen(example_str1) + 1);
	ck_assert(entry_item->text_encoding == PGQP_PLAINTEXT);
	ck_assert(entry_item->text_offset == test_offset);
	ck_assert(entry_item->usage_count == 1);

	entry_item = pgqp_store_text(example_str1, PGQP_SQLTEXT, 3, 0);
	ck_assert(entry_item);
	ck_assert(entry_item->text_offset == test_offset);
	ck_assert(entry_item->usage_count == 2);
	entry_item->usage_count = 0;

	test_offset += 3;
	pgqp_max_query_len = 1;
	entry_item = pgqp_store_text(example_str1, PGQP_SQLTEXT, 4, 0);
	ck_assert(entry_item);
	ck_assert(entry_item->origin_text_len == pgqp_max_query_len + 25);
	ck_assert(entry_item->text_len == entry_item->origin_text_len);
	ck_assert(entry_item->text_encoding == PGQP_PLAINTEXT);
	ck_assert(entry_item->text_offset == test_offset);
	ck_assert(entry_item->usage_count == 1);
	tmp_str = get_decoded_text(entry_item);
	ck_assert_str_eq(tmp_str, deleted_str);
	entry_item->usage_count = 0;

	test_offset += 26;
	pgqp_max_query_len = 1 * 1024;
	entry_item = pgqp_store_text(example_str2, PGQP_SQLTEXT, 5, 0);
	ck_assert(entry_item);
	ck_assert(entry_item->text_encoding == PGQP_PGLZ);
	ck_assert(entry_item->origin_text_len == strlen(example_str2) + 1);
	ck_assert(entry_item->text_offset == test_offset);
	ck_assert(entry_item->usage_count == 1);
	ck_assert(entry_item->text_len < entry_item->origin_text_len);
	tmp_str = get_decoded_text(entry_item);
	ck_assert_str_eq(tmp_str, example_str2);
	pfree(tmp_str);
	entry_item->usage_count = 0;

	test_offset += entry_item->text_len;
	entry_item = pgqp_store_text(example_str1, PGQP_SQLPLAN, 3, 1);
	ck_assert(entry_item);
	ck_assert(entry_item->text_offset == test_offset);
	ck_assert(entry_item->usage_count == 1);
	entry_item->usage_count = 0;

	test_offset +=3;
	entry_item = pgqp_store_text(example_str1, PGQP_GENSQLPLAN, 3, 1);
	ck_assert(entry_item);
	ck_assert(entry_item->text_offset == test_offset);
	ck_assert(entry_item->usage_count == 1);
	entry_item->usage_count = 0;

	test_offset +=3;
	pgqp_max_plan_len = 1;
	entry_item = pgqp_store_text(example_str2, PGQP_GENSQLPLAN, 3, 2);
	ck_assert(entry_item);
	ck_assert(entry_item->text_encoding == PGQP_PLAINTEXT);
	ck_assert(entry_item->origin_text_len == pgqp_max_plan_len + 27);
	ck_assert(entry_item->text_len == entry_item->origin_text_len);
	ck_assert(entry_item->text_offset == test_offset);
	ck_assert(entry_item->usage_count == 1);
	tmp_str = get_decoded_text(entry_item);
	ck_assert_str_eq(tmp_str, deleted_str2);
	entry_item->usage_count = 0;
}
END_TEST

START_TEST(test_need_gc)
{
	ck_assert(need_gc(false, 0, 0) == false);
	ck_assert(need_gc(false, 100*1024*1024, 0) == true);
}
END_TEST

START_TEST(test_pgqp_plan_dealloc)
{
	pgqpPlanEntry   *plan_entry = NULL;
	pgqpPlanHashKey  plan_key;
	HASH_SEQ_STATUS  hash_seq;
	pgqp_entry_reset(0, 0, 0);
	plan_key.userid = 1;
	plan_key.dbid = 1;
	plan_key.queryid = 3;
	plan_key.toplevel = true;
	for (int i=0; i < pgqp_max_plans; i++) {
		plan_key.planid = i;
		plan_entry = pgqp_plan_alloc(&plan_key, false, i);
		ck_assert(plan_entry);
		plan_entry->counters.usage = i;
	}
	pgqp_dealloc();
	// check the number of remaining items
	ck_assert(hash_get_num_entries(pgqp_plans) == pgqp_max_plans * (100 - FREE_PERCENT) / 100);
	// check if only last used items is in place
	hash_seq_init(&hash_seq, pgqp_plans);
	while ((plan_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		ck_assert(plan_entry->key.planid >= pgqp_max_plans * FREE_PERCENT / 100);
	}
}
END_TEST

START_TEST(test_pgqp_sql_dealloc)
{
	pgqpEntry   	  *entry = NULL;
	pgqpQueryHashKey  key;
	HASH_SEQ_STATUS   hash_seq;
	pgqp_entry_reset(0, 0, 0);
	key.userid = 1;
	key.dbid = 1;
	key.toplevel = true;
	for (int i=0; i < pgqp_max; i++) {
		key.queryid = i;
		entry = pgqp_query_alloc(&key, false, i);
		ck_assert(entry);
		entry->counters.usage = i;
	}
	pgqp_dealloc();
	ck_assert(hash_get_num_entries(pgqp_queries) == pgqp_max * (100 - FREE_PERCENT) / 100);
	hash_seq_init(&hash_seq, pgqp_queries);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		ck_assert(entry->key.queryid >= pgqp_max * FREE_PERCENT / 100);
	}
}
END_TEST

START_TEST(test_pgqp_gc)
{
	char *example_str = "SELECT 0 , 1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 ,"
	"14 , 15 , 16 , 17 , 18 , 19 , 20 , 21 , 22 , 23 , 24 , 25 , 26 , 27 , 28 , 29 ,"
	"30 , 31 , 32 , 33 , 34 , 35 , 36 , 37 , 38 , 39 , 40 , 41 , 42 , 43 , 44 , 45 ,"
	"46 , 47 , 48 , 49 , 50 , 51 , 52 , 53 , 54 , 55 , 56 , 57 , 58 , 59 , 60 , 61 ,"
	"62 , 63 , 64 , 65 , 66 , 67 , 68 , 69 , 70 , 71 , 72 , 73 , 74 , 75 , 76 , 77 ,"
	"78 , 79 , 80 , 81 , 82 , 83 , 84 , 85 , 86 , 87 , 88 , 89 , 90 , 91 , 92 , 93 ,"
	"94 , 95 , 96 , 97 , 98 , 99;";
	pgqpTextStorageEntry*   entry_item;
	HASH_SEQ_STATUS         hash_seq;
	int old_pgqp_storage_memory = pgqp_storage_memory;
	pgqp_storage_memory = 1;
	pgqp_entry_reset(0, 0, 0);
	pgqp_storage_memory = 5049 + 1; // storage size for 100 elements
	int64 test_offset = 0;

	pgqp_max_query_len = 1 * 1024;
	pgqp_max_plan_len = 1 * 1024;
	pgqp_encoding = PGQP_PLAINTEXT;

	for (int i = 1; i<=100; i++) {
		char *stored_str;
		stored_str = (char *) malloc( i + 1 );
		strncpy(stored_str, example_str, i + 1 );
		stored_str[i] = '\0';
		entry_item = pgqp_store_text(stored_str, PGQP_SQLTEXT, i, 0);
		ck_assert(entry_item);
		ck_assert(entry_item->text_offset == test_offset);
		ck_assert(entry_item->usage_count == 1);
		if (i % 2 == 1) {
			entry_item->usage_count = 0;
		}
		test_offset += i + 1;
	}
	pgqp_gc_storage();
	hash_seq_init(&hash_seq, pgqp_texts);
	while ((entry_item = hash_seq_search(&hash_seq)) != NULL)
	{
		test_offset = ((3 +
entry_item->text_key.item_id_low - 1)*((entry_item->text_key.item_id_low - 2) / 2)/2);
		ck_assert(entry_item);
		ck_assert(entry_item->text_offset == test_offset);
		ck_assert(entry_item->usage_count == 1);
	}
	pgqp_storage_memory = old_pgqp_storage_memory;
}
END_TEST

Suite * pgqp_suite(void)
{
	Suite *s;
	TCase *tc_core;

	s = suite_create("PGQP");

	/* Core test case */
	tc_core = tcase_create("Core");

	tcase_add_test(tc_core, test_need_gc);
	tcase_add_test(tc_core, test_pgqp_alloc);
	tcase_add_test(tc_core, test_pgqp_text);
	tcase_add_test(tc_core, test_pgqp_gc);
	tcase_add_test(tc_core, test_pgqp_plan_dealloc);
	tcase_add_test(tc_core, test_pgqp_sql_dealloc);
	suite_add_tcase(s, tc_core);

	return s;
}

 int main(int argc, char *argv[])
 {
	PGShmemHeader *seghdr;
	Size          size;
	int           numSemas;
	PGShmemHeader *shim = NULL;
	IsUnderPostmaster = false;
	shmem_request_hook = pgqp_shmem_request;
	MemoryContextInit();
	InitStandaloneProcess(argv[0]);
	InitializeGUCOptions();
	SetDataDir(".");
	size = CalculateShmemSize(&numSemas);
	seghdr = PGSharedMemoryCreate(size, &shim);
	InitShmemAccess(seghdr);
	PGReserveSemaphores(numSemas);
	InitShmemAllocation();
	CreateLWLocks();
	InitShmemIndex();
	dsm_shmem_init();
	InitLocks();
	process_shmem_requests();

	printf("init my own structures \n");
	pgqp_max = 1000;
	pgqp_storage_memory = 100 * 1024 * 1024;
	pgqp_shmem_init();
	int number_failed;
	Suite *s;
	SRunner *sr;

	s = pgqp_suite();
	sr = srunner_create(s);

	srunner_run_all(sr, CK_NORMAL);
	number_failed = srunner_ntests_failed(sr);
	srunner_free(sr);
	return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
