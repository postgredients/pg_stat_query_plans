/*-------------------------------------------------------------------------
 *
 * pg_stat_query_plans.c
 *              Track statement planning and execution times as well as resource
 *              usage across a whole database cluster.
 *
 * Assert definitions - use them for enable assertions only in pgqp extension
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGQP_A
#define PGQP_A

# ifndef PGQP_ASSERT_CHECKING

#define pgqpAssert(condition)       ((void)true)
#define pgqpAssertMacro(condition)  ((void)true)

# else

#include <assert.h>
#define pgqpAssert(p) assert(p)
#define pgqpAssertMacro(p)  ((void) assert(p))

# endif

#endif
