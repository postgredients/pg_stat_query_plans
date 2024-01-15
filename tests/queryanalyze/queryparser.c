#include "postgres.h"

#include <ctype.h>
#include <float.h>
#include <math.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>
#include "utils/memutils.h"

#include "parser/parser.h"
#if PG_VERSION_NUM < 160000
#include "parser/gram.h"
#else
#include "gram_pg16.h"
#endif
#include "parser/scanner.h"
#include "nodes/print.h"
#include "lib/stringinfo.h"

#include "common/pg_lzcompress.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define QUERY_FILE "test.txt"

const char* progname = "queryparser";
static bool need_replace(int token);
StringInfo _internal_parse(char* query, int* bind_prefix);
bool do_parse(char* query, char* (*output_fnc)(const void*) );

typedef struct {
	int k;
	char *v;
} Key;

#define MAP_SIZE 489

Key map[MAP_SIZE] = {
{ 0, "YYEOF" },
{ 256, "YYerror" },
{ 257, "YYUNDEF" },
{ 258, "IDENT" },
{ 259, "UIDENT" },
{ 260, "FCONST" },
{ 261, "SCONST" },
{ 262, "USCONST" },
{ 263, "BCONST" },
{ 264, "XCONST" },
{ 265, "Op" },
{ 266, "ICONST" },
{ 267, "PARAM" },
{ 268, "TYPECAST" },
{ 269, "DOT_DOT" },
{ 270, "COLON_EQUALS" },
{ 271, "EQUALS_GREATER" },
{ 272, "LESS_EQUALS" },
{ 273, "GREATER_EQUALS" },
{ 274, "NOT_EQUALS" },
{ 275, "ABORT_P" },
{ 276, "ABSOLUTE_P" },
{ 277, "ACCESS" },
{ 278, "ACTION" },
{ 279, "ADD_P" },
{ 280, "ADMIN" },
{ 281, "AFTER" },
{ 282, "AGGREGATE" },
{ 283, "ALL" },
{ 284, "ALSO" },
{ 285, "ALTER" },
{ 286, "ALWAYS" },
{ 287, "ANALYSE" },
{ 288, "ANALYZE" },
{ 289, "AND" },
{ 290, "ANY" },
{ 291, "ARRAY" },
{ 292, "AS" },
{ 293, "ASC" },
{ 294, "ASENSITIVE" },
{ 295, "ASSERTION" },
{ 296, "ASSIGNMENT" },
{ 297, "ASYMMETRIC" },
{ 298, "ATOMIC" },
{ 299, "AT" },
{ 300, "ATTACH" },
{ 301, "ATTRIBUTE" },
{ 302, "AUTHORIZATION" },
{ 303, "BACKWARD" },
{ 304, "BEFORE" },
{ 305, "BEGIN_P" },
{ 306, "BETWEEN" },
{ 307, "BIGINT" },
{ 308, "BINARY" },
{ 309, "BIT" },
{ 310, "BOOLEAN_P" },
{ 311, "BOTH" },
{ 312, "BREADTH" },
{ 313, "BY" },
{ 314, "CACHE" },
{ 315, "CALL" },
{ 316, "CALLED" },
{ 317, "CASCADE" },
{ 318, "CASCADED" },
{ 319, "CASE" },
{ 320, "CAST" },
{ 321, "CATALOG_P" },
{ 322, "CHAIN" },
{ 323, "CHAR_P" },
{ 324, "CHARACTER" },
{ 325, "CHARACTERISTICS" },
{ 326, "CHECK" },
{ 327, "CHECKPOINT" },
{ 328, "CLASS" },
{ 329, "CLOSE" },
{ 330, "CLUSTER" },
{ 331, "COALESCE" },
{ 332, "COLLATE" },
{ 333, "COLLATION" },
{ 334, "COLUMN" },
{ 335, "COLUMNS" },
{ 336, "COMMENT" },
{ 337, "COMMENTS" },
{ 338, "COMMIT" },
{ 339, "COMMITTED" },
{ 340, "COMPRESSION" },
{ 341, "CONCURRENTLY" },
{ 342, "CONFIGURATION" },
{ 343, "CONFLICT" },
{ 344, "CONNECTION" },
{ 345, "CONSTRAINT" },
{ 346, "CONSTRAINTS" },
{ 347, "CONTENT_P" },
{ 348, "CONTINUE_P" },
{ 349, "CONVERSION_P" },
{ 350, "COPY" },
{ 351, "COST" },
{ 352, "CREATE" },
{ 353, "CROSS" },
{ 354, "CSV" },
{ 355, "CUBE" },
{ 356, "CURRENT_P" },
{ 357, "CURRENT_CATALOG" },
{ 358, "CURRENT_DATE" },
{ 359, "CURRENT_ROLE" },
{ 360, "CURRENT_SCHEMA" },
{ 361, "CURRENT_TIME" },
{ 362, "CURRENT_TIMESTAMP" },
{ 363, "CURRENT_USER" },
{ 364, "CURSOR" },
{ 365, "CYCLE" },
{ 366, "DATA_P" },
{ 367, "DATABASE" },
{ 368, "DAY_P" },
{ 369, "DEALLOCATE" },
{ 370, "DEC" },
{ 371, "DECIMAL_P" },
{ 372, "DECLARE" },
{ 373, "DEFAULT" },
{ 374, "DEFAULTS" },
{ 375, "DEFERRABLE" },
{ 376, "DEFERRED" },
{ 377, "DEFINER" },
{ 378, "DELETE_P" },
{ 379, "DELIMITER" },
{ 380, "DELIMITERS" },
{ 381, "DEPENDS" },
{ 382, "DEPTH" },
{ 383, "DESC" },
{ 384, "DETACH" },
{ 385, "DICTIONARY" },
{ 386, "DISABLE_P" },
{ 387, "DISCARD" },
{ 388, "DISTINCT" },
{ 389, "DO" },
{ 390, "DOCUMENT_P" },
{ 391, "DOMAIN_P" },
{ 392, "DOUBLE_P" },
{ 393, "DROP" },
{ 394, "EACH" },
{ 395, "ELSE" },
{ 396, "ENABLE_P" },
{ 397, "ENCODING" },
{ 398, "ENCRYPTED" },
{ 399, "END_P" },
{ 400, "ENUM_P" },
{ 401, "ESCAPE" },
{ 402, "EVENT" },
{ 403, "EXCEPT" },
{ 404, "EXCLUDE" },
{ 405, "EXCLUDING" },
{ 406, "EXCLUSIVE" },
{ 407, "EXECUTE" },
{ 408, "EXISTS" },
{ 409, "EXPLAIN" },
{ 410, "EXPRESSION" },
{ 411, "EXTENSION" },
{ 412, "EXTERNAL" },
{ 413, "EXTRACT" },
{ 414, "FALSE_P" },
{ 415, "FAMILY" },
{ 416, "FETCH" },
{ 417, "FILTER" },
{ 418, "FINALIZE" },
{ 419, "FIRST_P" },
{ 420, "FLOAT_P" },
{ 421, "FOLLOWING" },
{ 422, "FOR" },
{ 423, "FORCE" },
{ 424, "FOREIGN" },
{ 425, "FORWARD" },
{ 426, "FREEZE" },
{ 427, "FROM" },
{ 428, "FULL" },
{ 429, "FUNCTION" },
{ 430, "FUNCTIONS" },
{ 431, "GENERATED" },
{ 432, "GLOBAL" },
{ 433, "GRANT" },
{ 434, "GRANTED" },
{ 435, "GREATEST" },
{ 436, "GROUP_P" },
{ 437, "GROUPING" },
{ 438, "GROUPS" },
{ 439, "HANDLER" },
{ 440, "HAVING" },
{ 441, "HEADER_P" },
{ 442, "HOLD" },
{ 443, "HOUR_P" },
{ 444, "IDENTITY_P" },
{ 445, "IF_P" },
{ 446, "ILIKE" },
{ 447, "IMMEDIATE" },
{ 448, "IMMUTABLE" },
{ 449, "IMPLICIT_P" },
{ 450, "IMPORT_P" },
{ 451, "IN_P" },
{ 452, "INCLUDE" },
{ 453, "INCLUDING" },
{ 454, "INCREMENT" },
{ 455, "INDEX" },
{ 456, "INDEXES" },
{ 457, "INHERIT" },
{ 458, "INHERITS" },
{ 459, "INITIALLY" },
{ 460, "INLINE_P" },
{ 461, "INNER_P" },
{ 462, "INOUT" },
{ 463, "INPUT_P" },
{ 464, "INSENSITIVE" },
{ 465, "INSERT" },
{ 466, "INSTEAD" },
{ 467, "INT_P" },
{ 468, "INTEGER" },
{ 469, "INTERSECT" },
{ 470, "INTERVAL" },
{ 471, "INTO" },
{ 472, "INVOKER" },
{ 473, "IS" },
{ 474, "ISNULL" },
{ 475, "ISOLATION" },
{ 476, "JOIN" },
{ 477, "KEY" },
{ 478, "LABEL" },
{ 479, "LANGUAGE" },
{ 480, "LARGE_P" },
{ 481, "LAST_P" },
{ 482, "LATERAL_P" },
{ 483, "LEADING" },
{ 484, "LEAKPROOF" },
{ 485, "LEAST" },
{ 486, "LEFT" },
{ 487, "LEVEL" },
{ 488, "LIKE" },
{ 489, "LIMIT" },
{ 490, "LISTEN" },
{ 491, "LOAD" },
{ 492, "LOCAL" },
{ 493, "LOCALTIME" },
{ 494, "LOCALTIMESTAMP" },
{ 495, "LOCATION" },
{ 496, "LOCK_P" },
{ 497, "LOCKED" },
{ 498, "LOGGED" },
{ 499, "MAPPING" },
{ 500, "MATCH" },
{ 501, "MATCHED" },
{ 502, "MATERIALIZED" },
{ 503, "MAXVALUE" },
{ 504, "MERGE" },
{ 505, "METHOD" },
{ 506, "MINUTE_P" },
{ 507, "MINVALUE" },
{ 508, "MODE" },
{ 509, "MONTH_P" },
{ 510, "MOVE" },
{ 511, "NAME_P" },
{ 512, "NAMES" },
{ 513, "NATIONAL" },
{ 514, "NATURAL" },
{ 515, "NCHAR" },
{ 516, "NEW" },
{ 517, "NEXT" },
{ 518, "NFC" },
{ 519, "NFD" },
{ 520, "NFKC" },
{ 521, "NFKD" },
{ 522, "NO" },
{ 523, "NONE" },
{ 524, "NORMALIZE" },
{ 525, "NORMALIZED" },
{ 526, "NOT" },
{ 527, "NOTHING" },
{ 528, "NOTIFY" },
{ 529, "NOTNULL" },
{ 530, "NOWAIT" },
{ 531, "NULL_P" },
{ 532, "NULLIF" },
{ 533, "NULLS_P" },
{ 534, "NUMERIC" },
{ 535, "OBJECT_P" },
{ 536, "OF" },
{ 537, "OFF" },
{ 538, "OFFSET" },
{ 539, "OIDS" },
{ 540, "OLD" },
{ 541, "ON" },
{ 542, "ONLY" },
{ 543, "OPERATOR" },
{ 544, "OPTION" },
{ 545, "OPTIONS" },
{ 546, "OR" },
{ 547, "ORDER" },
{ 548, "ORDINALITY" },
{ 549, "OTHERS" },
{ 550, "OUT_P" },
{ 551, "OUTER_P" },
{ 552, "OVER" },
{ 553, "OVERLAPS" },
{ 554, "OVERLAY" },
{ 555, "OVERRIDING" },
{ 556, "OWNED" },
{ 557, "OWNER" },
{ 558, "PARALLEL" },
{ 559, "PARAMETER" },
{ 560, "PARSER" },
{ 561, "PARTIAL" },
{ 562, "PARTITION" },
{ 563, "PASSING" },
{ 564, "PASSWORD" },
{ 565, "PLACING" },
{ 566, "PLANS" },
{ 567, "POLICY" },
{ 568, "POSITION" },
{ 569, "PRECEDING" },
{ 570, "PRECISION" },
{ 571, "PRESERVE" },
{ 572, "PREPARE" },
{ 573, "PREPARED" },
{ 574, "PRIMARY" },
{ 575, "PRIOR" },
{ 576, "PRIVILEGES" },
{ 577, "PROCEDURAL" },
{ 578, "PROCEDURE" },
{ 579, "PROCEDURES" },
{ 580, "PROGRAM" },
{ 581, "PUBLICATION" },
{ 582, "QUOTE" },
{ 583, "RANGE" },
{ 584, "READ" },
{ 585, "REAL" },
{ 586, "REASSIGN" },
{ 587, "RECHECK" },
{ 588, "RECURSIVE" },
{ 589, "REF" },
{ 590, "REFERENCES" },
{ 591, "REFERENCING" },
{ 592, "REFRESH" },
{ 593, "REINDEX" },
{ 594, "RELATIVE_P" },
{ 595, "RELEASE" },
{ 596, "RENAME" },
{ 597, "REPEATABLE" },
{ 598, "REPLACE" },
{ 599, "REPLICA" },
{ 600, "RESET" },
{ 601, "RESTART" },
{ 602, "RESTRICT" },
{ 603, "RETURN" },
{ 604, "RETURNING" },
{ 605, "RETURNS" },
{ 606, "REVOKE" },
{ 607, "RIGHT" },
{ 608, "ROLE" },
{ 609, "ROLLBACK" },
{ 610, "ROLLUP" },
{ 611, "ROUTINE" },
{ 612, "ROUTINES" },
{ 613, "ROW" },
{ 614, "ROWS" },
{ 615, "RULE" },
{ 616, "SAVEPOINT" },
{ 617, "SCHEMA" },
{ 618, "SCHEMAS" },
{ 619, "SCROLL" },
{ 620, "SEARCH" },
{ 621, "SECOND_P" },
{ 622, "SECURITY" },
{ 623, "SELECT" },
{ 624, "SEQUENCE" },
{ 625, "SEQUENCES" },
{ 626, "SERIALIZABLE" },
{ 627, "SERVER" },
{ 628, "SESSION" },
{ 629, "SESSION_USER" },
{ 630, "SET" },
{ 631, "SETS" },
{ 632, "SETOF" },
{ 633, "SHARE" },
{ 634, "SHOW" },
{ 635, "SIMILAR" },
{ 636, "SIMPLE" },
{ 637, "SKIP" },
{ 638, "SMALLINT" },
{ 639, "SNAPSHOT" },
{ 640, "SOME" },
{ 641, "SQL_P" },
{ 642, "STABLE" },
{ 643, "STANDALONE_P" },
{ 644, "START" },
{ 645, "STATEMENT" },
{ 646, "STATISTICS" },
{ 647, "STDIN" },
{ 648, "STDOUT" },
{ 649, "STORAGE" },
{ 650, "STORED" },
{ 651, "STRICT_P" },
{ 652, "STRIP_P" },
{ 653, "SUBSCRIPTION" },
{ 654, "SUBSTRING" },
{ 655, "SUPPORT" },
{ 656, "SYMMETRIC" },
{ 657, "SYSID" },
{ 658, "SYSTEM_P" },
{ 659, "TABLE" },
{ 660, "TABLES" },
{ 661, "TABLESAMPLE" },
{ 662, "TABLESPACE" },
{ 663, "TEMP" },
{ 664, "TEMPLATE" },
{ 665, "TEMPORARY" },
{ 666, "TEXT_P" },
{ 667, "THEN" },
{ 668, "TIES" },
{ 669, "TIME" },
{ 670, "TIMESTAMP" },
{ 671, "TO" },
{ 672, "TRAILING" },
{ 673, "TRANSACTION" },
{ 674, "TRANSFORM" },
{ 675, "TREAT" },
{ 676, "TRIGGER" },
{ 677, "TRIM" },
{ 678, "TRUE_P" },
{ 679, "TRUNCATE" },
{ 680, "TRUSTED" },
{ 681, "TYPE_P" },
{ 682, "TYPES_P" },
{ 683, "UESCAPE" },
{ 684, "UNBOUNDED" },
{ 685, "UNCOMMITTED" },
{ 686, "UNENCRYPTED" },
{ 687, "UNION" },
{ 688, "UNIQUE" },
{ 689, "UNKNOWN" },
{ 690, "UNLISTEN" },
{ 691, "UNLOGGED" },
{ 692, "UNTIL" },
{ 693, "UPDATE" },
{ 694, "USER" },
{ 695, "USING" },
{ 696, "VACUUM" },
{ 697, "VALID" },
{ 698, "VALIDATE" },
{ 699, "VALIDATOR" },
{ 700, "VALUE_P" },
{ 701, "VALUES" },
{ 702, "VARCHAR" },
{ 703, "VARIADIC" },
{ 704, "VARYING" },
{ 705, "VERBOSE" },
{ 706, "VERSION_P" },
{ 707, "VIEW" },
{ 708, "VIEWS" },
{ 709, "VOLATILE" },
{ 710, "WHEN" },
{ 711, "WHERE" },
{ 712, "WHITESPACE_P" },
{ 713, "WINDOW" },
{ 714, "WITH" },
{ 715, "WITHIN" },
{ 716, "WITHOUT" },
{ 717, "WORK" },
{ 718, "WRAPPER" },
{ 719, "WRITE" },
{ 720, "XML_P" },
{ 721, "XMLATTRIBUTES" },
{ 722, "XMLCONCAT" },
{ 723, "XMLELEMENT" },
{ 724, "XMLEXISTS" },
{ 725, "XMLFOREST" },
{ 726, "XMLNAMESPACES" },
{ 727, "XMLPARSE" },
{ 728, "XMLPI" },
{ 729, "XMLROOT" },
{ 730, "XMLSERIALIZE" },
{ 731, "XMLTABLE" },
{ 732, "YEAR_P" },
{ 733, "YES_P" },
{ 734, "ZONE" },
{ 735, "NOT_LA" },
{ 736, "NULLS_LA" },
{ 737, "WITH_LA" },
{ 738, "MODE_TYPE_NAME" },
{ 739, "MODE_PLPGSQL_EXPR" },
{ 740, "MODE_PLPGSQL_ASSIGN1" },
{ 741, "MODE_PLPGSQL_ASSIGN2" },
{ 742, "MODE_PLPGSQL_ASSIGN3" },
{ 743, "UMINUS" }
};


bool need_replace(int token)
{
	return 	(token == FCONST)
			|| (token == ICONST)
			|| (token == SCONST)
			|| (token == USCONST)
			|| (token == BCONST)
			|| (token == XCONST);
}

StringInfo _internal_parse(char* query, int* bind_prefix)
{
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE         yylloc;
	int tok;
    char * bind_prefix_str;
    YYLTYPE last_yylloc = 0;
    int last_tok = 0;
	char *last_str = NULL;
	StringInfo plan_out;

	plan_out = makeStringInfo();

	yyscanner = scanner_init(query,
                             &yyextra,
                             &ScanKeywords,
                             ScanKeywordTokens
							 );
    for (;;)
    {
        tok = core_yylex(&yylval, &yylloc, yyscanner);

        if (need_replace(last_tok))
        {
            int bind_len=asprintf(&bind_prefix_str, "$%i", *bind_prefix);
			(*bind_prefix)++;
			if (bind_len == 0)
				break;
            appendStringInfoString(plan_out, bind_prefix_str);
            free(bind_prefix_str);
        }
		else if (1 < 0 && last_tok == IDENT && query[last_yylloc] == '"' && yylloc - last_yylloc > 2)
		{
			StringInfo new_ident;
			int end_pos  = 1;
			while(query[yylloc - end_pos] != '"')
			    end_pos++;
			last_str = strndup((char *) query + last_yylloc + 1, yylloc -
last_yylloc - end_pos - 1);
			new_ident = _internal_parse(last_str, bind_prefix);
			free(last_str);
			appendStringInfoString(plan_out, "\"");
			appendStringInfoString(plan_out, new_ident->data);
			last_str = strndup((char *) query + yylloc - end_pos, end_pos);
			appendStringInfoString(plan_out, last_str);
			free(last_str);
		}
        else
        {
            last_str = strndup((char *) query + last_yylloc, yylloc - last_yylloc);
            appendStringInfoString(plan_out, last_str);
            free(last_str);
        }
        if (tok == 0)
            break;
        last_tok = tok;
        last_yylloc = yylloc;

    }
    scanner_finish(yyscanner);

	return plan_out;
}

bool do_parse(char* query, char* (*output_fnc)(const void*) )
{
	int bind_prefix = 1;
	MemoryContext ctx = NULL;
	StringInfo plan_out;
	char *compressed_dst;
	char *uncompressed_dst;
	int len;
	int dlen;

	ctx = AllocSetContextCreate(TopMemoryContext,
								"RootContext",
								ALLOCSET_DEFAULT_MINSIZE,
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(ctx);

	plan_out = _internal_parse(query, &bind_prefix);

	// printf("%s\n", plan_out->data);


	compressed_dst = palloc(plan_out->len);
	uncompressed_dst = palloc(plan_out->len);
    len = pglz_compress(plan_out->data,
                        plan_out->len,
                        compressed_dst,
                        PGLZ_strategy_default);

	if (len > 0)
	{
		printf("initial size - %i compressed size - %i\n", plan_out->len, len);

		/*
		dlen = pglz_decompress(compressed_dst,
							   len,
							   uncompressed_dst,
							   plan_out->len,
							   true);

		if (dlen < 0)
			printf("decompess failed \n");
		else
			printf("%s \n", uncompressed_dst);
		*/
	}

	MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextDelete(ctx);

	return true;
}

#define BUFSIZE 32768

int main(int argc, char **argv)
{
	char  line[BUFSIZE];
	FILE *fp;
	long lSize;

	MemoryContextInit();

	fp = fopen ( QUERY_FILE  , "rb" );

	fseek( fp , 0L , SEEK_END);
	lSize = ftell( fp );
	rewind( fp );

	Assert(lSize < BUFSIZE);

	if (fread( line , lSize, 1 , fp) == 0)
		return 1;
	line[lSize] = '\0';
	fclose(fp);

	if (line[0] == '#' || line[0] == '\0')
		return 1;

	for (int i =0; i < 1000; i++)
	{
		do_parse(line, &nodeToString);
	}
}
