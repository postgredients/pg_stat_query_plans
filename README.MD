pg_stat_query_plans
==============

Yet another pg_stat_plans 

Features
--------

The pg_stat_query_plans module provides a means for tracking execution statistics of SQL statements and their plans.

Differences from original pg_stat_query_plans(https://www.postgresql.org/docs/current/pgstatstatements.html):
- Track query execution plans 
- Store SQL and their execution plans in memory
- Compress stored texts
- Limit the maximum query and execution plan size
- Store one (first meet) unchanged SQL for each execution plan 
- Add generation (uint64) field to all entities to cope with ABA problem

Installation
============

Compiling
---------

The module can be built using the standard PGXS infrastructure. For this to
work, the ``pg_config`` program must be available in your $PATH. Instruction to
install follows::

    git clone https://github.com/leborchuk/pg_stat_query_plans.git
    cd pg_stat_query_plans
    make
    make install

PostgreSQL setup
----------------

The extension is now available. But, as it requires some shared memory to hold
its counters, the module must be loaded at PostgreSQL startup. Thus, you must
add the module to ``shared_preload_libraries`` in your ``postgresql.conf``. You
need a server restart to take the change into account. 

Add the following parameters into you ``postgresql.conf``:

    # postgresql.conf
    shared_preload_libraries = 'pg_stat_query_plans'

Once your PostgreSQL cluster is restarted, you can install the extension in
every database where you need to access the statistics::

    mydb=# CREATE EXTENSION pg_stat_query_plans;

pg_stat_query_plans Usage
===========================

pg_stat_query_plans create several objects.

pg_stat_query_plans view
--------------------------

The statistics gathered by the module are made available via a view named pg_stat_query_plans. This view contains one row for each distinct combination of database ID, user ID, query ID and whether it's a top-level statement or not (up to the maximum number of distinct statements that the module can track). The columns of the view are shown in Table 1.

Table 1. pg_stat_query_plans Columns

| Name                   | Type             | Description                                                                                                                  |
| ---------------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| userid                 | oid              | OID of user who executed the statement                                                                                       |
| dbid                   | oid              | OID of database in which the statement was executed                                                                          |
| toplevel               | bool             | True if the query was executed as a top-level statement (always true if pg_stat_query_plans.track is set to top)              |
| queryid                | bigint           | Hash code to identify identical normalized queries                                                                           |
| query                  | text             | Text of a representative statement                                                                                           |
| plans                  | bigint           | Number of times the statement was planned (if pg_stat_query_plans.track_planning is enabled, otherwise zero)                  |
| total_plan_time        | double precision | Total time spent planning the statement, in milliseconds (if pg_stat_query_plans.track_planning is enabled, otherwise zero)   |
| min_plan_time          | double precision | Minimum time spent planning the statement, in milliseconds (if pg_stat_query_plans.track_planning is enabled, otherwise zero) |
| max_plan_time          | double precision | Maximum time spent planning the statement, in milliseconds (if pg_stat_query_plans.track_planning is enabled, otherwise zero) |
| mean_plan_time         | double precision | Mean time spent planning the statement, in milliseconds (if pg_stat_query_plans.track_planning is enabled, otherwise zero)    |
| stddev_plan_time       | double precision | Population standard deviation of time spent planning the statement, in milliseconds                                          |
| calls                  | bigint           | Number of times the statement was executed                                                                                   |
| total_exec_time        | double precision | Total time spent executing the statement, in milliseconds                                                                    |
| min_exec_time          | double precision | Minimum time spent executing the statement, in milliseconds                                                                  |
| max_exec_time          | double precision | Maximum time spent executing the statement, in milliseconds                                                                  |
| mean_exec_time         | double precision | Mean time spent executing the statement, in milliseconds                                                                     |
| stddev_exec_time       | double precision | Population standard deviation of time spent executing the statement, in milliseconds                                         |
| rows                   | bigint           | Total number of rows retrieved or affected by the statement                                                                  |
| shared_blks_hit        | bigint           | Total number of shared block cache hits by the statement                                                                     |
| shared_blks_read       | bigint           | Total number of shared blocks read by the statement                                                                          |
| shared_blks_dirtied    | bigint           | Total number of shared blocks dirtied by the statement                                                                       |
| shared_blks_written    | bigint           | Total number of shared blocks written by the statement                                                                       |
| local_blks_hit         | bigint           | Total number of local block cache hits by the statement                                                                      |
| local_blks_read        | bigint           | Total number of local blocks read by the statement                                                                           |
| local_blks_dirtied     | bigint           | Total number of local blocks dirtied by the statement                                                                        |
| local_blks_written     | bigint           | Total number of local blocks written by the statement                                                                        |
| temp_blks_read         | bigint           | Total number of temp blocks read by the statement                                                                            |
| temp_blks_written      | bigint           | Total number of temp blocks written by the statement                                                                         |
| blk_read_time          | double precision | Total time the statement spent reading data file blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)     |
| blk_write_time         | double precision | Total time the statement spent writing data file blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)     |
| temp_blk_read_time     | double precision | Total time the statement spent reading temporary file blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)|
| temp_blk_write_time    | double precision | Total time the statement spent writing temporary file blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)|
| wal_records            | bigint           | Total number of WAL records generated by the statement                                                                       |
| wal_fpi                | bigint           | Total number of WAL full page images generated by the statement                                                              |
| wal_bytes              | numeric          | Total amount of WAL generated by the statement in bytes                                                                      |
| jit_functions          | bigint           | Total number of functions JIT-compiled by the statement                                                                      |
| jit_generation_time    | double precision | Total time spent by the statement on generating JIT code, in milliseconds                                                    |
| jit_inlining_count     | bigint           | Number of times functions have been inlined                                                                                  |
| jit_inlining_time      | double precision | Total time spent by the statement on inlining functions, in milliseconds                                                     |
| jit_optimization_count | bigint           | Number of times the statement has been optimized                                                                             |
| jit_optimization_time  | double precision | Total time spent by the statement on optimizing, in milliseconds                                                             |
| jit_emission_count     | bigint           | Number of times code has been emitted                                                                                        |
| jit_emission_time      | double precision | Total time spent by the statement on emitting code, in milliseconds                                                          |
| generation             | bigint           | Sequence number, increase each time query registered, so always change if query was deallocated and re-added                 |


pg_stat_query_plans_plans view
--------------------------------

pg_stat_query_plans_plans show detailed info about statements gathered by
pg_stat_query_plans - which execution plan was used to process statements, how
many times statement has been executed using specific execution plan, etc. This
view contains one row for each distinct combination of database ID, user ID,
query ID, plan ID and whether it's a top-level statement or not (up to the
maximum number of distinct statements that the module can track). The columns of
the view are shown in Table 2.

Table 2. pg_stat_query_plans_plans Columns

| Name                   | Type             | Description                                                                                                                  |
| ---------------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| userid                 | oid              | OID of user who executed the statement                                                                                       |
| dbid                   | oid              | OID of database in which the statement was executed                                                                          |
| toplevel               | bool             | True if the query was executed as a top-level statement (always true if pg_stat_query_plans.track is set to top)              |
| queryid                | bigint           | Hash code to identify identical normalized queries                                                                           |
| planid                 | bigint           | Hash code to identify identical normalized query execution plans                                                             |
| query                  | text             | Example of query statement with literals (collected at the first execution)                                                  |
| normalized_plan        | text             | Text of a representative execution plan                                                                                      |
| example_plan           | text             | Example of query execution plan with costs and literals (collected at the first execution)                                   |
| calls                  | bigint           | Number of times the statement was executed                                                                                   |
| total_exec_time        | double precision | Total time spent executing the statement, in milliseconds                                                                    |
| min_exec_time          | double precision | Minimum time spent executing the statement, in milliseconds                                                                  |
| max_exec_time          | double precision | Maximum time spent executing the statement, in milliseconds                                                                  |
| mean_exec_time         | double precision | Mean time spent executing the statement, in milliseconds                                                                     |
| stddev_exec_time       | double precision | Population standard deviation of time spent executing the statement, in milliseconds                                         |
| rows                   | bigint           | Total number of rows retrieved or affected by the statement                                                                  |
| shared_blks_hit        | bigint           | Total number of shared block cache hits by the statement                                                                     |
| shared_blks_read       | bigint           | Total number of shared blocks read by the statement                                                                          |
| shared_blks_dirtied    | bigint           | Total number of shared blocks dirtied by the statement                                                                       |
| shared_blks_written    | bigint           | Total number of shared blocks written by the statement                                                                       |
| local_blks_hit         | bigint           | Total number of local block cache hits by the statement                                                                      |
| local_blks_read        | bigint           | Total number of local blocks read by the statement                                                                           |
| local_blks_dirtied     | bigint           | Total number of local blocks dirtied by the statement                                                                        |
| local_blks_written     | bigint           | Total number of local blocks written by the statement                                                                        |
| temp_blks_read         | bigint           | Total number of temp blocks read by the statement                                                                            |
| temp_blks_written      | bigint           | Total number of temp blocks written by the statement                                                                         |
| blk_read_time          | double precision | Total time the statement spent reading data file blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)     |
| blk_write_time         | double precision | Total time the statement spent writing data file blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)     |
| temp_blk_read_time     | double precision | Total time the statement spent reading temporary file blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)|
| temp_blk_write_time    | double precision | Total time the statement spent writing temporary file blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)|
| wal_records            | bigint           | Total number of WAL records generated by the statement                                                                       |
| wal_fpi                | bigint           | Total number of WAL full page images generated by the statement                                                              |
| wal_bytes              | numeric          | Total amount of WAL generated by the statement in bytes                                                                      |
| jit_functions          | bigint           | Total number of functions JIT-compiled by the statement                                                                      |
| jit_generation_time    | double precision | Total time spent by the statement on generating JIT code, in milliseconds                                                    |
| jit_inlining_count     | bigint           | Number of times functions have been inlined                                                                                  |
| jit_inlining_time      | double precision | Total time spent by the statement on inlining functions, in milliseconds                                                     |
| jit_optimization_count | bigint           | Number of times the statement has been optimized                                                                             |
| jit_optimization_time  | double precision | Total time spent by the statement on optimizing, in milliseconds                                                             |
| jit_emission_count     | bigint           | Number of times code has been emitted                                                                                        |
| jit_emission_time      | double precision | Total time spent by the statement on emitting code, in milliseconds                                                          |
| startup_cost           | bigint           | The amount of work (cost) expended before output scan can start                                                              |
| total_cost             | bigint           | Estimated cost if all rows were to be retrieved                                                                              |
| plan_rows              | bigint           | The number of rows (estimation) output by the plan if executed to completion                                                 |
| plan_width             | bigint           | Average width of rows in bytes (estimation)                                                                                  |
| generation             | bigint           | Sequence number, increase each time execution plam registered, so always change if plan was deallocated and re-added         |


pg_stat_query_plans_info_ya view
-------------------------------

The statistics of the pg_stat_query_plans module itself are tracked and made available via a view named pg_stat_query_plans_info_ya. This view contains only a single row. The columns of the view are shown in Table 3:


Table 3. pg_stat_query_plans_info_ya Columns

| Name                         | Type      | Description                                                                                                                                    |
| ---------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| dealloc                      | bigint    | Total number of times entries about the least-executed statements were deallocated because more distinct max statements were stored            |
| stats_reset                  | timestamp | Time at which all statistics in the pg_stat_query_plans view were last reset                                                                 |
| gc_count                     | bigint    | Total number of times texts (of queries and execution plans) were compacted to reclaim memory which was allocated, but is no longer referenced |
| generation                   | bigint    | Increasing sequence counter, used to mark by unique identifier added statements                                                                |
| storage_offset               | bigint    | Amount of memory, used to store texts (of queries and execution plans), include allocated but no longer used space                             |
| queries_text_size            | bigint    | Total size of stored query texts                                                                                                               |
| queries_compressed_text_size | bigint    | Total size, used in shared storage for query texts (by default texts compressed before store)                                                  |
| plans_text_size              | bigint    | Total size of stored query execution plans (stored as explain output)                                                                          |
| plans_compressed_text_size   | bigint    | Total size, used in shared storage for query execution plans (by default explain output compressed before store)                               |
| queries                      | bigint    | Total number of stored unique query statements                                                                                                 |
| execution_plans              | bigint    | Total number of stored unique query execution plan statements                                                                                  |
| queries_wiped_out            | bigint    | The number of query statements deleted in deallocation                                                                                         |
| plans_wiped_out              | bigint    | The number of query execution plan statements deleted in deallocation                                                                          |
| dealloc_time_ms              | bigint    | Total time spent in deallocation process, in ms                                                                                                |
| gc_time_ms                   | bigint    | Total time spent in gc process, in ms                                                                                                          |


pg_stat_query_plans functions
-------------------------------

### pg_stat_query_plans_reset(userid Oid, dbid Oid, queryid bigint) returns void

 pg_stat_query_plans_reset_ya discards statistics (statements and execution plans) gathered so far by pg_stat_query_plans corresponding to the specified userid, dbid and queryid. If any of the parameters are not specified, the default value 0 is used for each of them and the statistics that match with other parameters will be reset. If no parameter is specified or all the specified parameters are 0, it will discard all statistics. If all statistics in the pg_stat_query_plans view are discarded, it will also reset the statistics in the pg_stat_query_plans_info view. By default, this function can only be executed by superusers. Access may be granted to others using GRANT.


### pg_stat_query_plans_reset_minmax() returns void

 Reset for all statements and execution plans statistics min_plan_time, max_plan_time, mean_plan_time, stddev_plan_time, min_exec_time, max_exec_time, ean_exec_time, stddev_exec_time to zero for pg_stat_staments_ya and min_exec_time, max_exec_time, ean_exec_time, stddev_exec_time to zero for pg_stat_query_plans. Could be usefull for measure planning and execution time deviations between statistics observations. 


### pg_stat_query_plans(showtext boolean) returns setof record

 The pg_stat_query_plans view is defined in terms of a function also named pg_stat_query_plans. It is possible for clients to call the pg_stat_query_plans function directly, and by specifying showtext := false have query text be omitted (that is, the OUT argument that corresponds to the view's query column will return nulls). This feature is intended to support external tools that might wish to avoid the overhead of repeatedly retrieving query texts of indeterminate length. Such tools can instead cache the first query text observed for each entry themselves, since that is all pg_stat_query_plans itself does, and then retrieve query texts only as needed.


### pg_stat_query_plans_plans(showtext boolean) returns setof record

 Same as pg_stat_query_plans, return pg_stat_query_plans_plans view data



pg_stat_query_plans configuration parameters
----------------------------------------------

The module requires additional shared memory proportional to pg_stat_query_plans.max and pg_stat_query_plans.storage_memory. Note that this memory is consumed whenever the module is loaded, even if pg_stat_query_plans.track is set to none.

These parameters must be set in ``postgresql.conf``. Typical usage might be:

    # postgresql.conf
    shared_preload_libraries = 'pg_stat_query_plans'
    pg_stat_query_plans.max = 10000
    pg_stat_query_plans.storage_memory = 268435456
    pg_stat_query_plans.track = all


### pg_stat_query_plans.max (integer)

 The maximum number of statements tracked by the module (i.e., the maximum number of rows in the pg_stat_query_plans view). If more distinct statements than that are observed, information about the least-executed statements is discarded. The number of times such information was discarded can be seen in the pg_stat_query_plans_info_ya view. The default value is 10000 for query statements and 2 x pg_stat_query_plans.max for execution plan entries. This parameter can only be set at server start.


### pg_stat_query_plans.max_query_len (integer)

 The maximum length of query texts stored by pg_stat_query_plans. Query length counts after compression. In a case of exceeded maximum length text will be truncated till max_query_len symbols and message "deleted tail xx symbols" will be added to the end of query text. The default value is 4194304. Only superusers can change this setting.


### pg_stat_query_plans.max_plan_size (integer)

 The maximum length of execution plan texts stored by pg_stat_query_plans. Execution plan length counts after compression. In a case of exceeded maximum length text will be truncated till max_plan_size symbols and message "deleted tail xx symbols" will be added to the end of execution plan. The default value is 6291456. Only superusers can change this setting.


### pg_stat_query_plans.storage_memory (integer)

 The amount of shared memory used to store texts - queries and execution plans. The default value is 268435456. Only superusers can change this setting.


### pg_stat_query_plans.track (enum)

 Controls which statements are counted by the module. Specify top to track top-level statements (those issued directly by clients), all to also track nested statements (such as statements invoked within functions), or none to disable statement statistics collection. The default value is top. Only superusers can change this setting.


### pg_stat_query_plans.track_utility (bool)

 Controls whether utility commands are tracked by the module. Utility commands are all those other than SELECT, INSERT, UPDATE, DELETE, and MERGE. The default value is on. Only superusers can change this setting.


### pg_stat_query_plans.track_planning (bool)

 Сontrols whether planning operations and duration are tracked by the module. Enabling this parameter may incur a noticeable performance penalty, especially when statements with identical query structure are executed by many concurrent connections which compete to update a small number of pg_stat_query_plans entries. The default value is off. Only superusers can change this setting.


### pg_stat_query_plans.track_plans (bool)

 Controls whether track execution plans or not. If tracking execution plans query statistics will be stored twice - for query and corresponding execution plan. The default value is on. Only superusers can change this setting.


### pg_stat_query_plans.normalize_plans (bool)

 Enable execution plan normalization. Normalization is quite similar query text normalization: literal constants or fluctuating values such like costs or measured time replaced by substitute variables.  The default value is on. Only superusers can change this setting.


### pg_stat_query_plans.pgqp_encoding (enum)

 Set compression method for stored texts, "pglz" compress texts using PGLZ algorithm, "plaintext" stores texts without compression. The default value is "pglz". Only superusers can change this setting.


### pg_stat_query_plans.example_plan_format (enum)

 Selects which format to be appied for plan representation in pg_stat_query_plans. Example plan collected during first query execution using EXPLAIN statement. Possible values for pg_stat_query_plans.example_plan_format is the same as for EXPLAIN foutput format, which can be "text", "xml", "json", or "yaml". The default value is "json". Only superusers can change this setting.


### pg_stat_query_plans.example_plan_verbose (bool)

 Set VERBOSE for EXPLAIN on collecting example plan. The default value is on. Only superusers can change this setting.


### pg_stat_query_plans.example_plan_triggers (bool)

 Log trigger trace in EXPLAIN. 



Sample output
=============

    test=# SELECT pg_stat_query_plans_reset_ya();

    $ pgbench -i test
    $ pgbench --client=10 --jobs=5 --time=120 test

    test=# \x

    test=# SELECT query, calls, total_exec_time, rows, 100.0 * shared_blks_hit /
    test-#                nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
    test-#           FROM pg_stat_query_plans ORDER BY total_exec_time DESC LIMIT 5;

    -[ RECORD 1 ]---+-----------------------------------------------------------------------------------------------------
    query           | UPDATE pgbench_tellers SET tbalance = tbalance + $1 WHERE tid = $2
    calls           | 525503
    total_exec_time | 1976767.946711934
    rows            | 525503
    hit_percent     | 99.9999744183500245
    -[ RECORD 2 ]---+-----------------------------------------------------------------------------------------------------
    query           | UPDATE pgbench_branches SET bbalance = bbalance + $1 WHERE bid = $2
    calls           | 525490
    total_exec_time | 1956168.140791016
    rows            | 525490
    hit_percent     | 99.9999879581690878
    -[ RECORD 3 ]---+-----------------------------------------------------------------------------------------------------
    query           | UPDATE pgbench_accounts SET abalance = abalance + $1 WHERE aid = $2
    calls           | 525505
    total_exec_time | 19382.711481000042
    rows            | 525505
    hit_percent     | 99.9897051941974731
    -[ RECORD 4 ]---+-----------------------------------------------------------------------------------------------------
    query           | SELECT abalance FROM pgbench_accounts WHERE aid = $1
    calls           | 525503
    total_exec_time | 6356.537003000069
    rows            | 525503
    hit_percent     | 100.0000000000000000
    -[ RECORD 5 ]---+-----------------------------------------------------------------------------------------------------
    query           | INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
    calls           | 525477
    total_exec_time | 5549.626488000205
    rows            | 525477
    hit_percent     | 99.9981238062717404
    
    test=# SELECT query, normalized_plan, calls, total_exec_time, rows, 100.0 * shared_blks_hit /
    test-#                nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
    test-#           FROM pg_stat_query_plans_plans ORDER BY total_exec_time DESC LIMIT 5;

    -[ RECORD 1 ]---+------------------------------------------------------------------------
    query           | UPDATE pgbench_branches SET bbalance = bbalance + $1 WHERE bid = $2
    normalized_plan | Update on public.pgbench_branches                                      +
                    |   ->  Index Scan using pgbench_branches_pkey on public.pgbench_branches+
                    |         Output: (bbalance + $1::integer), ctid                         +
                    |         Index Cond: (pgbench_branches.bid = $2)                        +
                    |
    calls           | 259532
    total_exec_time | 966260.7150509937
    rows            | 259532
    hit_percent     | 100.0000000000000000
    -[ RECORD 2 ]---+------------------------------------------------------------------------
    query           | UPDATE pgbench_branches SET bbalance = bbalance + $1 WHERE bid = $2
    normalized_plan | Update on public.pgbench_branches                                      +
                    |   ->  Index Scan using pgbench_branches_pkey on public.pgbench_branches+
                    |         Output: (bbalance + $1), ctid                                  +
                    |         Index Cond: (pgbench_branches.bid = $2)                        +
                    |
    calls           | 259284
    total_exec_time | 963860.7137359913
    rows            | 259284
    hit_percent     | 100.0000000000000000
    -[ RECORD 3 ]---+------------------------------------------------------------------------
    query           | UPDATE pgbench_tellers SET tbalance = tbalance + $1 WHERE tid = $2
    normalized_plan | Update on public.pgbench_tellers                                       +
                    |   ->  Index Scan using pgbench_tellers_pkey on public.pgbench_tellers  +
                    |         Output: (tbalance + $1), ctid                                  +
                    |         Index Cond: (pgbench_tellers.tid = $2)                         +
                    |
    calls           | 189256
    total_exec_time | 713699.6042899912
    rows            | 189256
    hit_percent     | 100.0000000000000000
    -[ RECORD 4 ]---+------------------------------------------------------------------------
    query           | UPDATE pgbench_tellers SET tbalance = tbalance + $1 WHERE tid = $2
    normalized_plan | Update on public.pgbench_tellers                                       +
                    |   ->  Index Scan using pgbench_tellers_pkey on public.pgbench_tellers  +
                    |         Output: (tbalance + $1::integer), ctid                         +
                    |         Index Cond: (pgbench_tellers.tid = $2)                         +
                    |
    calls           | 189010
    total_exec_time | 711204.459061006
    rows            | 189010
    hit_percent     | 100.0000000000000000
    -[ RECORD 5 ]---+------------------------------------------------------------------------
    query           | UPDATE pgbench_tellers SET tbalance = tbalance + $1 WHERE tid = $2
    normalized_plan | Update on public.pgbench_tellers                                       +
                    |   ->  Bitmap Heap Scan on public.pgbench_tellers                       +
                    |         Output: (tbalance + $1::integer), ctid                         +
                    |         Recheck Cond: (pgbench_tellers.tid = $2)                       +
                    |         ->  Bitmap Index Scan on pgbench_tellers_pkey                  +
                    |               Index Cond: (pgbench_tellers.tid = $3)                   +
                    |
    calls           | 68881
    total_exec_time | 257161.87226300067
    rows            | 68881
    hit_percent     | 100.0000000000000000



Description
===========

Similar extensions
------------------

There are a lot of similar extensions, here the links for some of them:

1. pg_show_plans https://github.com/cybertec-postgresql/pg_show_plans Extension that shows query plans of all the currently running SQL statements.

2. pg_store_plans https://github.com/ossc-db/pg_store_plans Provides a means for tracking execution plan statistics of all SQL statements executed by a server (need pg_stat_query_plans to get query text).

3. pg_stat_monitor https://github.com/percona/pg_stat_monitor Query Performance Monitoring tool for PostgreSQL. It attempts to provide a more holistic picture by providing much-needed query performance insights in a single view. 


Calculating PlanID
------------------

An execution plan identifier (PlanID) allows to merge similar execution plans. PlanID is a hash of the query execution plan without insignificant details such as cost, numeric and string literals. To calculate PlanID, the Explain Output is re-parsed, algorithm of work is:
1. In the ExecutorEnd hook, print the query execution plan (without cost)
2. Start yyscann of execution plan
3. In a parsing process, if we encounter a constant (numeric or string), we replace it with a symbol of the form $<serial number>
4. Leave the rest of the elements unchanged
5. Calculate the hash from the resulting execution plan
This approach allows one not to depend on Explain output specificities various PG versions.
It would be more CPU efficient to calculate the ID based on QueryDesc.plannedstmt without printing the plan, but this was left for future improvements.


Deleting old plans
------------------

The data is stored in several structures:
1. Continuous memory area for storing query texts and plan texts
2. Hash table of query statistics
3. Hash table of execution plan statistics

When purge is performed, execution plans are sorted by usage (similar to pg_stat_query_plans). Remove PGQP_FREE_PERCENT of the least used execution plans. During the deletion process, the reference counts in the query statistics and text storage are reduced. Then remove queries from the hash table with zero reference counts.

If the cause of cleanup was text memory area exhaustion, cleanup continues until PGQP_FREE_PERCENT memory size will be freed.

If, after cleaning, a high watermark in the text storage area does not allow inserting new text, garbage collection is taking place in text storage (texts are moved, links to them are replaced).


Bugs and limitations
====================

Support PG>=11

Tests work only for PG15

Authors
=======

pg_stat_query_plans is an original development from Leonid Borchuk, with large
portions of code inspired from pg_stat_query_plans and pg_stat_plans. 

Very thanks to Peter Geoghegan and Itagaki Takahiroso I could write this extension quite straightforward.


License
=======

pg_stat_query_plans is free software distributed under the Apache License, Version 2.0.

Copyright (c) 2024, YANDEX LLC
