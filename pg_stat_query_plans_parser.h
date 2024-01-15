#ifndef PGSTATSTATEMENSYAP_H
#define PGSTATSTATEMENSYAP_H

#if PG_VERSION_NUM < 140000
#define JUMBLE_SIZE                             1024    /* query serialization buffer size */

/*
 * Struct for tracking locations/lengths of constants during normalization
 */
typedef struct pgqpLocationLen
{
	int                     location;               /* start offset in query text */
	int                     length;                 /* length in bytes, or -1 to ignore */
} pgqpLocationLen;

/*
 * Working state for computing a query jumble and producing a normalized
 * query string
 */
typedef struct pgqpJumbleState
{
	/* Jumble of current query tree */
	unsigned char *jumble;

	/* Number of bytes used in jumble[] */
	Size            jumble_len;

	/* Array of locations of constants that should be removed */
	pgqpLocationLen *clocations;

	/* Allocated length of clocations array */
	int                     clocations_buf_size;

	/* Current number of valid entries in clocations array */
	int                     clocations_count;

	/* highest Param id we've seen, in order to start normalization correctly */
	int                     highest_extern_param_id;
} pgqpJumbleState;

void AppendJumble(pgqpJumbleState *jstate,
						 const unsigned char *item, Size size);
void JumbleQuery(pgqpJumbleState *jstate, Query *query);
void JumbleRangeTable(pgqpJumbleState *jstate, List *rtable);
void JumbleRowMarks(pgqpJumbleState *jstate, List *rowMarks);
void JumbleExpr(pgqpJumbleState *jstate, Node *node);
void RecordConstLocation(pgqpJumbleState *jstate, int location);
char *gen_normquery(pgqpJumbleState *jstate, const char *query,
					int query_loc, int *query_len_p);
void fill_in_constant_lengths(pgqpJumbleState *jstate, const char *query,
							  int query_loc);

#else
char *gen_normquery(JumbleState *jstate, const char *query,
					int query_loc, int *query_len_p);

void fill_in_constant_lengths(JumbleState *jstate, const char *query,
							  int query_loc);
#endif

int comp_location(const void *a, const void *b);

#endif

StringInfo gen_normplan(char *execution_plan);
