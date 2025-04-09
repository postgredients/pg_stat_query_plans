#include "postgres.h"
#if PG_VERSION_NUM >= 130000
#include "common/hashfn.h"
#elif PG_VERSION_NUM >= 120000
#include "utils/hashutils.h"
#else
#include "access/hash.h"
#endif
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/scanner.h"
#include "parser/scansup.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#if PG_VERSION_NUM >= 160000
#include "gram_pg16.h"
#else
#include "parser/gram.h"
#endif

#include "pg_stat_query_plans_parser.h"
#include "pg_stat_query_plans_assert.h"

#if PG_VERSION_NUM < 140000

/*
 * pgqpAppendJumble: Append a value that is substantive in a given query to
 * the current jumble.
 */
void pgqpAppendJumble(pgqpJumbleState *jstate, const unsigned char *item,
                  Size size) {
  unsigned char *jumble = jstate->jumble;
  Size jumble_len = jstate->jumble_len;

  /*
   * Whenever the jumble buffer is full, we hash the current contents and
   * reset the buffer to contain just that hash value, thus relying on the
   * hash to summarize everything so far.
   */
  while (size > 0) {
    Size part_size;

    if (jumble_len >= JUMBLE_SIZE) {
      uint64 start_hash;

      start_hash = DatumGetUInt64(hash_any_extended(jumble, JUMBLE_SIZE, 0));
      memcpy(jumble, &start_hash, sizeof(start_hash));
      jumble_len = sizeof(start_hash);
    }
    part_size = Min(size, JUMBLE_SIZE - jumble_len);
    memcpy(jumble + jumble_len, item, part_size);
    jumble_len += part_size;
    item += part_size;
    size -= part_size;
  }
  jstate->jumble_len = jumble_len;
}

/*
 * Wrappers around pgqpAppendJumble to encapsulate details of serialization
 * of individual local variable elements.
 */
#define PGQP_APP_JUMB(item)                                                         \
  pgqpAppendJumble(jstate, (const unsigned char *)&(item), sizeof(item))
#define PGQP_APP_JUMB_STRING(str)                                                   \
  pgqpAppendJumble(jstate, (const unsigned char *)(str), strlen(str) + 1)

/*
 * pgqpJumbleQuery: Selectively serialize the query tree, appending significant
 * data to the "query jumble" while ignoring nonsignificant data.
 *
 * Rule of thumb for what to include is that we should ignore anything not
 * semantically significant (such as alias names) as well as anything that can
 * be deduced from child nodes (else we'd just be double-hashing that piece
 * of information).
 */
void pgqpJumbleQuery(pgqpJumbleState *jstate, Query *query) {
  pgqpAssert(IsA(query, Query));
  pgqpAssert(query->utilityStmt == NULL);

  PGQP_APP_JUMB(query->commandType);
  /* resultRelation is usually predictable from commandType */
  pgqpJumbleExpr(jstate, (Node *)query->cteList);
  pgqpJumbleRangeTable(jstate, query->rtable);
  pgqpJumbleExpr(jstate, (Node *)query->jointree);
  pgqpJumbleExpr(jstate, (Node *)query->targetList);
  pgqpJumbleExpr(jstate, (Node *)query->onConflict);
  pgqpJumbleExpr(jstate, (Node *)query->returningList);
  pgqpJumbleExpr(jstate, (Node *)query->groupClause);
  pgqpJumbleExpr(jstate, (Node *)query->groupingSets);
  pgqpJumbleExpr(jstate, query->havingQual);
  pgqpJumbleExpr(jstate, (Node *)query->windowClause);
  pgqpJumbleExpr(jstate, (Node *)query->distinctClause);
  pgqpJumbleExpr(jstate, (Node *)query->sortClause);
  pgqpJumbleExpr(jstate, query->limitOffset);
  pgqpJumbleExpr(jstate, query->limitCount);
  pgqpJumbleRowMarks(jstate, query->rowMarks);
  pgqpJumbleExpr(jstate, query->setOperations);
}

/*
 * Jumble a range table
 */
void pgqpJumbleRangeTable(pgqpJumbleState *jstate, List *rtable) {
  ListCell *lc;

  foreach (lc, rtable) {
    RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

    PGQP_APP_JUMB(rte->rtekind);
    switch (rte->rtekind) {
    case RTE_RELATION:
      PGQP_APP_JUMB(rte->relid);
      pgqpJumbleExpr(jstate, (Node *)rte->tablesample);
      break;
    case RTE_SUBQUERY:
      pgqpJumbleQuery(jstate, rte->subquery);
      break;
    case RTE_JOIN:
      PGQP_APP_JUMB(rte->jointype);
      break;
    case RTE_FUNCTION:
      pgqpJumbleExpr(jstate, (Node *)rte->functions);
      break;
    case RTE_TABLEFUNC:
      pgqpJumbleExpr(jstate, (Node *)rte->tablefunc);
      break;
    case RTE_VALUES:
      pgqpJumbleExpr(jstate, (Node *)rte->values_lists);
      break;
    case RTE_CTE:

      /*
       * Depending on the CTE name here isn't ideal, but it's the
       * only info we have to identify the referenced WITH item.
       */
      PGQP_APP_JUMB_STRING(rte->ctename);
      PGQP_APP_JUMB(rte->ctelevelsup);
      break;
    case RTE_NAMEDTUPLESTORE:
      PGQP_APP_JUMB_STRING(rte->enrname);
      break;
#if PG_VERSION_NUM >= 120000
    case RTE_RESULT:
      break;
#endif
    default:
      elog(ERROR, "unrecognized RTE kind: %d", (int)rte->rtekind);
      break;
    }
  }
}

/*
 * Jumble a rowMarks list
 */
void pgqpJumbleRowMarks(pgqpJumbleState *jstate, List *rowMarks) {
  ListCell *lc;

  foreach (lc, rowMarks) {
    RowMarkClause *rowmark = lfirst_node(RowMarkClause, lc);

    if (!rowmark->pushedDown) {
      PGQP_APP_JUMB(rowmark->rti);
      PGQP_APP_JUMB(rowmark->strength);
      PGQP_APP_JUMB(rowmark->waitPolicy);
    }
  }
}

/*
 * Jumble an expression tree
 *
 * In general this function should handle all the same node types that
 * expression_tree_walker() does, and therefore it's coded to be as parallel
 * to that function as possible.  However, since we are only invoked on
 * queries immediately post-parse-analysis, we need not handle node types
 * that only appear in planning.
 *
 * Note: the reason we don't simply use expression_tree_walker() is that the
 * point of that function is to support tree walkers that don't care about
 * most tree node types, but here we care about all types.  We should complain
 * about any unrecognized node type.
 */
void pgqpJumbleExpr(pgqpJumbleState *jstate, Node *node) {
  ListCell *temp;

  if (node == NULL)
    return;

  /* Guard against stack overflow due to overly complex expressions */
  check_stack_depth();

  /*
   * We always emit the node's NodeTag, then any additional fields that are
   * considered significant, and then we recurse to any child nodes.
   */
  PGQP_APP_JUMB(node->type);

  switch (nodeTag(node)) {
  case T_Var: {
    Var *var = (Var *)node;

    PGQP_APP_JUMB(var->varno);
    PGQP_APP_JUMB(var->varattno);
    PGQP_APP_JUMB(var->varlevelsup);
  } break;
  case T_Const: {
    Const *c = (Const *)node;

    /* We jumble only the constant's type, not its value */
    PGQP_APP_JUMB(c->consttype);
    /* Also, record its parse location for query normalization */
    pgqpRecordConstLocation(jstate, c->location);
  } break;
  case T_Param: {
    Param *p = (Param *)node;

    PGQP_APP_JUMB(p->paramkind);
    PGQP_APP_JUMB(p->paramid);
    PGQP_APP_JUMB(p->paramtype);
    /* Also, track the highest external Param id */
    if (p->paramkind == PARAM_EXTERN &&
        p->paramid > jstate->highest_extern_param_id)
      jstate->highest_extern_param_id = p->paramid;
  } break;
  case T_Aggref: {
    Aggref *expr = (Aggref *)node;

    PGQP_APP_JUMB(expr->aggfnoid);
    pgqpJumbleExpr(jstate, (Node *)expr->aggdirectargs);
    pgqpJumbleExpr(jstate, (Node *)expr->args);
    pgqpJumbleExpr(jstate, (Node *)expr->aggorder);
    pgqpJumbleExpr(jstate, (Node *)expr->aggdistinct);
    pgqpJumbleExpr(jstate, (Node *)expr->aggfilter);
  } break;
  case T_GroupingFunc: {
    GroupingFunc *grpnode = (GroupingFunc *)node;

    pgqpJumbleExpr(jstate, (Node *)grpnode->refs);
  } break;
  case T_WindowFunc: {
    WindowFunc *expr = (WindowFunc *)node;

    PGQP_APP_JUMB(expr->winfnoid);
    PGQP_APP_JUMB(expr->winref);
    pgqpJumbleExpr(jstate, (Node *)expr->args);
    pgqpJumbleExpr(jstate, (Node *)expr->aggfilter);
  } break;
#if PG_VERSION_NUM >= 120000
  case T_SubscriptingRef: {
    SubscriptingRef *sbsref = (SubscriptingRef *)node;

    pgqpJumbleExpr(jstate, (Node *)sbsref->refupperindexpr);
    pgqpJumbleExpr(jstate, (Node *)sbsref->reflowerindexpr);
    pgqpJumbleExpr(jstate, (Node *)sbsref->refexpr);
    pgqpJumbleExpr(jstate, (Node *)sbsref->refassgnexpr);
  } break;
#else
  case T_ArrayRef: {
    ArrayRef *aref = (ArrayRef *)node;

    pgqpJumbleExpr(jstate, (Node *)aref->refupperindexpr);
    pgqpJumbleExpr(jstate, (Node *)aref->reflowerindexpr);
    pgqpJumbleExpr(jstate, (Node *)aref->refexpr);
    pgqpJumbleExpr(jstate, (Node *)aref->refassgnexpr);
  } break;
#endif
  case T_FuncExpr: {
    FuncExpr *expr = (FuncExpr *)node;

    PGQP_APP_JUMB(expr->funcid);
    pgqpJumbleExpr(jstate, (Node *)expr->args);
  } break;
  case T_NamedArgExpr: {
    NamedArgExpr *nae = (NamedArgExpr *)node;

    PGQP_APP_JUMB(nae->argnumber);
    pgqpJumbleExpr(jstate, (Node *)nae->arg);
  } break;
  case T_OpExpr:
  case T_DistinctExpr: /* struct-equivalent to OpExpr */
  case T_NullIfExpr:   /* struct-equivalent to OpExpr */
  {
    OpExpr *expr = (OpExpr *)node;

    PGQP_APP_JUMB(expr->opno);
    pgqpJumbleExpr(jstate, (Node *)expr->args);
  } break;
  case T_ScalarArrayOpExpr: {
    ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *)node;

    PGQP_APP_JUMB(expr->opno);
    PGQP_APP_JUMB(expr->useOr);
    pgqpJumbleExpr(jstate, (Node *)expr->args);
  } break;
  case T_BoolExpr: {
    BoolExpr *expr = (BoolExpr *)node;

    PGQP_APP_JUMB(expr->boolop);
    pgqpJumbleExpr(jstate, (Node *)expr->args);
  } break;
  case T_SubLink: {
    SubLink *sublink = (SubLink *)node;

    PGQP_APP_JUMB(sublink->subLinkType);
    PGQP_APP_JUMB(sublink->subLinkId);
    pgqpJumbleExpr(jstate, (Node *)sublink->testexpr);
    pgqpJumbleQuery(jstate, castNode(Query, sublink->subselect));
  } break;
  case T_FieldSelect: {
    FieldSelect *fs = (FieldSelect *)node;

    PGQP_APP_JUMB(fs->fieldnum);
    pgqpJumbleExpr(jstate, (Node *)fs->arg);
  } break;
  case T_FieldStore: {
    FieldStore *fstore = (FieldStore *)node;

    pgqpJumbleExpr(jstate, (Node *)fstore->arg);
    pgqpJumbleExpr(jstate, (Node *)fstore->newvals);
  } break;
  case T_RelabelType: {
    RelabelType *rt = (RelabelType *)node;

    PGQP_APP_JUMB(rt->resulttype);
    pgqpJumbleExpr(jstate, (Node *)rt->arg);
  } break;
  case T_CoerceViaIO: {
    CoerceViaIO *cio = (CoerceViaIO *)node;

    PGQP_APP_JUMB(cio->resulttype);
    pgqpJumbleExpr(jstate, (Node *)cio->arg);
  } break;
  case T_ArrayCoerceExpr: {
    ArrayCoerceExpr *acexpr = (ArrayCoerceExpr *)node;

    PGQP_APP_JUMB(acexpr->resulttype);
    pgqpJumbleExpr(jstate, (Node *)acexpr->arg);
    pgqpJumbleExpr(jstate, (Node *)acexpr->elemexpr);
  } break;
  case T_ConvertRowtypeExpr: {
    ConvertRowtypeExpr *crexpr = (ConvertRowtypeExpr *)node;

    PGQP_APP_JUMB(crexpr->resulttype);
    pgqpJumbleExpr(jstate, (Node *)crexpr->arg);
  } break;
  case T_CollateExpr: {
    CollateExpr *ce = (CollateExpr *)node;

    PGQP_APP_JUMB(ce->collOid);
    pgqpJumbleExpr(jstate, (Node *)ce->arg);
  } break;
  case T_CaseExpr: {
    CaseExpr *caseexpr = (CaseExpr *)node;

    pgqpJumbleExpr(jstate, (Node *)caseexpr->arg);
    foreach (temp, caseexpr->args) {
      CaseWhen *when = lfirst_node(CaseWhen, temp);

      pgqpJumbleExpr(jstate, (Node *)when->expr);
      pgqpJumbleExpr(jstate, (Node *)when->result);
    }
    pgqpJumbleExpr(jstate, (Node *)caseexpr->defresult);
  } break;
  case T_CaseTestExpr: {
    CaseTestExpr *ct = (CaseTestExpr *)node;

    PGQP_APP_JUMB(ct->typeId);
  } break;
  case T_ArrayExpr:
    pgqpJumbleExpr(jstate, (Node *)((ArrayExpr *)node)->elements);
    break;
  case T_RowExpr:
    pgqpJumbleExpr(jstate, (Node *)((RowExpr *)node)->args);
    break;
  case T_RowCompareExpr: {
    RowCompareExpr *rcexpr = (RowCompareExpr *)node;

    PGQP_APP_JUMB(rcexpr->rctype);
    pgqpJumbleExpr(jstate, (Node *)rcexpr->largs);
    pgqpJumbleExpr(jstate, (Node *)rcexpr->rargs);
  } break;
  case T_CoalesceExpr:
    pgqpJumbleExpr(jstate, (Node *)((CoalesceExpr *)node)->args);
    break;
  case T_MinMaxExpr: {
    MinMaxExpr *mmexpr = (MinMaxExpr *)node;

    PGQP_APP_JUMB(mmexpr->op);
    pgqpJumbleExpr(jstate, (Node *)mmexpr->args);
  } break;
  case T_SQLValueFunction: {
    SQLValueFunction *svf = (SQLValueFunction *)node;

    PGQP_APP_JUMB(svf->op);
    /* type is fully determined by op */
    PGQP_APP_JUMB(svf->typmod);
  } break;
  case T_XmlExpr: {
    XmlExpr *xexpr = (XmlExpr *)node;

    PGQP_APP_JUMB(xexpr->op);
    pgqpJumbleExpr(jstate, (Node *)xexpr->named_args);
    pgqpJumbleExpr(jstate, (Node *)xexpr->args);
  } break;
  case T_NullTest: {
    NullTest *nt = (NullTest *)node;

    PGQP_APP_JUMB(nt->nulltesttype);
    pgqpJumbleExpr(jstate, (Node *)nt->arg);
  } break;
  case T_BooleanTest: {
    BooleanTest *bt = (BooleanTest *)node;

    PGQP_APP_JUMB(bt->booltesttype);
    pgqpJumbleExpr(jstate, (Node *)bt->arg);
  } break;
  case T_CoerceToDomain: {
    CoerceToDomain *cd = (CoerceToDomain *)node;

    PGQP_APP_JUMB(cd->resulttype);
    pgqpJumbleExpr(jstate, (Node *)cd->arg);
  } break;
  case T_CoerceToDomainValue: {
    CoerceToDomainValue *cdv = (CoerceToDomainValue *)node;

    PGQP_APP_JUMB(cdv->typeId);
  } break;
  case T_SetToDefault: {
    SetToDefault *sd = (SetToDefault *)node;

    PGQP_APP_JUMB(sd->typeId);
  } break;
  case T_CurrentOfExpr: {
    CurrentOfExpr *ce = (CurrentOfExpr *)node;

    PGQP_APP_JUMB(ce->cvarno);
    if (ce->cursor_name)
      PGQP_APP_JUMB_STRING(ce->cursor_name);
    PGQP_APP_JUMB(ce->cursor_param);
  } break;
  case T_NextValueExpr: {
    NextValueExpr *nve = (NextValueExpr *)node;

    PGQP_APP_JUMB(nve->seqid);
    PGQP_APP_JUMB(nve->typeId);
  } break;
  case T_InferenceElem: {
    InferenceElem *ie = (InferenceElem *)node;

    PGQP_APP_JUMB(ie->infercollid);
    PGQP_APP_JUMB(ie->inferopclass);
    pgqpJumbleExpr(jstate, ie->expr);
  } break;
  case T_TargetEntry: {
    TargetEntry *tle = (TargetEntry *)node;

    PGQP_APP_JUMB(tle->resno);
    PGQP_APP_JUMB(tle->ressortgroupref);
    pgqpJumbleExpr(jstate, (Node *)tle->expr);
  } break;
  case T_RangeTblRef: {
    RangeTblRef *rtr = (RangeTblRef *)node;

    PGQP_APP_JUMB(rtr->rtindex);
  } break;
  case T_JoinExpr: {
    JoinExpr *join = (JoinExpr *)node;

    PGQP_APP_JUMB(join->jointype);
    PGQP_APP_JUMB(join->isNatural);
    PGQP_APP_JUMB(join->rtindex);
    pgqpJumbleExpr(jstate, join->larg);
    pgqpJumbleExpr(jstate, join->rarg);
    pgqpJumbleExpr(jstate, join->quals);
  } break;
  case T_FromExpr: {
    FromExpr *from = (FromExpr *)node;

    pgqpJumbleExpr(jstate, (Node *)from->fromlist);
    pgqpJumbleExpr(jstate, from->quals);
  } break;
  case T_OnConflictExpr: {
    OnConflictExpr *conf = (OnConflictExpr *)node;

    PGQP_APP_JUMB(conf->action);
    pgqpJumbleExpr(jstate, (Node *)conf->arbiterElems);
    pgqpJumbleExpr(jstate, conf->arbiterWhere);
    pgqpJumbleExpr(jstate, (Node *)conf->onConflictSet);
    pgqpJumbleExpr(jstate, conf->onConflictWhere);
    PGQP_APP_JUMB(conf->constraint);
    PGQP_APP_JUMB(conf->exclRelIndex);
    pgqpJumbleExpr(jstate, (Node *)conf->exclRelTlist);
  } break;
  case T_List:
    foreach (temp, (List *)node) {
      pgqpJumbleExpr(jstate, (Node *)lfirst(temp));
    }
    break;
  case T_IntList:
    foreach (temp, (List *)node) {
      PGQP_APP_JUMB(lfirst_int(temp));
    }
    break;
  case T_SortGroupClause: {
    SortGroupClause *sgc = (SortGroupClause *)node;

    PGQP_APP_JUMB(sgc->tleSortGroupRef);
    PGQP_APP_JUMB(sgc->eqop);
    PGQP_APP_JUMB(sgc->sortop);
    PGQP_APP_JUMB(sgc->nulls_first);
  } break;
  case T_GroupingSet: {
    GroupingSet *gsnode = (GroupingSet *)node;

    pgqpJumbleExpr(jstate, (Node *)gsnode->content);
  } break;
  case T_WindowClause: {
    WindowClause *wc = (WindowClause *)node;

    PGQP_APP_JUMB(wc->winref);
    PGQP_APP_JUMB(wc->frameOptions);
    pgqpJumbleExpr(jstate, (Node *)wc->partitionClause);
    pgqpJumbleExpr(jstate, (Node *)wc->orderClause);
    pgqpJumbleExpr(jstate, wc->startOffset);
    pgqpJumbleExpr(jstate, wc->endOffset);
  } break;
  case T_CommonTableExpr: {
    CommonTableExpr *cte = (CommonTableExpr *)node;

    /* we store the string name because RTE_CTE RTEs need it */
    PGQP_APP_JUMB_STRING(cte->ctename);
#if PG_VERSION_NUM >= 120000
    PGQP_APP_JUMB(cte->ctematerialized);
#endif
    pgqpJumbleQuery(jstate, castNode(Query, cte->ctequery));
  } break;
  case T_SetOperationStmt: {
    SetOperationStmt *setop = (SetOperationStmt *)node;

    PGQP_APP_JUMB(setop->op);
    PGQP_APP_JUMB(setop->all);
    pgqpJumbleExpr(jstate, setop->larg);
    pgqpJumbleExpr(jstate, setop->rarg);
  } break;
  case T_RangeTblFunction: {
    RangeTblFunction *rtfunc = (RangeTblFunction *)node;

    pgqpJumbleExpr(jstate, rtfunc->funcexpr);
  } break;
  case T_TableFunc: {
    TableFunc *tablefunc = (TableFunc *)node;

    pgqpJumbleExpr(jstate, tablefunc->docexpr);
    pgqpJumbleExpr(jstate, tablefunc->rowexpr);
    pgqpJumbleExpr(jstate, (Node *)tablefunc->colexprs);
  } break;
  case T_TableSampleClause: {
    TableSampleClause *tsc = (TableSampleClause *)node;

    PGQP_APP_JUMB(tsc->tsmhandler);
    pgqpJumbleExpr(jstate, (Node *)tsc->args);
    pgqpJumbleExpr(jstate, (Node *)tsc->repeatable);
  } break;
  default:
    /* Only a warning, since we can stumble along anyway */
    elog(WARNING, "unrecognized node type: %d", (int)nodeTag(node));
    break;
  }
}

/*
 * Record location of constant within query string of query tree
 * that is currently being walked.
 */
void pgqpRecordConstLocation(pgqpJumbleState *jstate, int location) {
  /* -1 indicates unknown or undefined location */
  if (location >= 0) {
    /* enlarge array if needed */
    if (jstate->clocations_count >= jstate->clocations_buf_size) {
      jstate->clocations_buf_size *= 2;
      jstate->clocations = (pgqpLocationLen *)repalloc(
          jstate->clocations,
          jstate->clocations_buf_size * sizeof(pgqpLocationLen));
    }
    jstate->clocations[jstate->clocations_count].location = location;
    /* initialize lengths to -1 to simplify pgqp_fill_in_constant_lengths */
    jstate->clocations[jstate->clocations_count].length = -1;
    jstate->clocations_count++;
  }
}

/*
 * Generate a normalized version of the query string that will be used to
 * represent all similar queries.
 *
 * Note that the normalized representation may well vary depending on
 * just which "equivalent" query is used to create the hashtable entry.
 * We assume this is OK.
 *
 * If query_loc > 0, then "query" has been advanced by that much compared to
 * the original string start, so we need to translate the provided locations
 * to compensate.  (This lets us avoid re-scanning statements before the one
 * of interest, so it's worth doing.)
 *
 * *query_len_p contains the input string length, and is updated with
 * the result string length on exit.  The resulting string might be longer
 * or shorter depending on what happens with replacement of constants.
 *
 * Returns a palloc'd string.
 */
char *pgqp_gen_normquery(pgqpJumbleState *jstate, const char *query, int query_loc,
                    int *query_len_p) {
  char *norm_query;
  int query_len = *query_len_p;
  int i, norm_query_buflen, /* Space allowed for norm_query */
      len_to_wrt,           /* Length (in bytes) to write */
      quer_loc = 0,         /* Source query byte location */
      n_quer_loc = 0,       /* Normalized query byte location */
      last_off = 0,         /* Offset from start for previous tok */
      last_tok_len = 0;     /* Length (in bytes) of that tok */

  /*
   * Get constants' lengths (core system only gives us locations).  Note
   * this also ensures the items are sorted by location.
   */
  pgqp_fill_in_constant_lengths(jstate, query, query_loc);

  /*
   * Allow for $n symbols to be longer than the constants they replace.
   * Constants must take at least one byte in text form, while a $n symbol
   * certainly isn't more than 11 bytes, even if n reaches INT_MAX.  We
   * could refine that limit based on the max value of n for the current
   * query, but it hardly seems worth any extra effort to do so.
   */
  norm_query_buflen = query_len + jstate->clocations_count * 10;

  /* Allocate result buffer */
  norm_query = palloc(norm_query_buflen + 1);

  for (i = 0; i < jstate->clocations_count; i++) {
    int off,     /* Offset from start for cur tok */
        tok_len; /* Length (in bytes) of that tok */

    off = jstate->clocations[i].location;
    /* Adjust recorded location if we're dealing with partial string */
    off -= query_loc;

    tok_len = jstate->clocations[i].length;

    if (tok_len < 0)
      continue; /* ignore any duplicates */

    /* Copy next chunk (what precedes the next constant) */
    len_to_wrt = off - last_off;
    len_to_wrt -= last_tok_len;

    pgqpAssert(len_to_wrt >= 0);
    memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
    n_quer_loc += len_to_wrt;

    /* And insert a param symbol in place of the constant token */
    n_quer_loc += sprintf(norm_query + n_quer_loc, "$%d",
                          i + 1 + jstate->highest_extern_param_id);

    quer_loc = off + tok_len;
    last_off = off;
    last_tok_len = tok_len;
  }

  /*
   * We've copied up until the last ignorable constant.  Copy over the
   * remaining bytes of the original query string.
   */
  len_to_wrt = query_len - quer_loc;

  pgqpAssert(len_to_wrt >= 0);
  memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
  n_quer_loc += len_to_wrt;

  pgqpAssert(n_quer_loc <= norm_query_buflen);
  norm_query[n_quer_loc] = '\0';

  *query_len_p = n_quer_loc;
  return norm_query;
}

/*
 * Given a valid SQL string and an array of constant-location records,
 * fill in the textual lengths of those constants.
 *
 * The constants may use any allowed constant syntax, such as float literals,
 * bit-strings, single-quoted strings and dollar-quoted strings.  This is
 * accomplished by using the public API for the core scanner.
 *
 * It is the caller's job to ensure that the string is a valid SQL statement
 * with constants at the indicated locations.  Since in practice the string
 * has already been parsed, and the locations that the caller provides will
 * have originated from within the authoritative parser, this should not be
 * a problem.
 *
 * Duplicate constant pointers are possible, and will have their lengths
 * marked as '-1', so that they are later ignored.  (Actually, we assume the
 * lengths were initialized as -1 to start with, and don't change them here.)
 *
 * If query_loc > 0, then "query" has been advanced by that much compared to
 * the original string start, so we need to translate the provided locations
 * to compensate.  (This lets us avoid re-scanning statements before the one
 * of interest, so it's worth doing.)
 *
 * N.B. There is an assumption that a '-' character at a Const location begins
 * a negative numeric constant.  This precludes there ever being another
 * reason for a constant to start with a '-'.
 */
void pgqp_fill_in_constant_lengths(pgqpJumbleState *jstate, const char *query,
                              int query_loc) {
  pgqpLocationLen *locs;
  core_yyscan_t yyscanner;
  core_yy_extra_type yyextra;
  core_YYSTYPE yylval;
  YYLTYPE yylloc;
  int last_loc = -1;
  int i;

  /*
   * Sort the records by location so that we can process them in order while
   * scanning the query text.
   */
  if (jstate->clocations_count > 1)
    qsort(jstate->clocations, jstate->clocations_count, sizeof(pgqpLocationLen),
          pgqp_comp_location);
  locs = jstate->clocations;

  /* initialize the flex scanner --- should match raw_parser() */
  yyscanner = scanner_init(query, &yyextra,
#if PG_VERSION_NUM >= 120000
                           &ScanKeywords, ScanKeywordTokens
#else
                           ScanKeywords, NumScanKeywords
#endif
  );

  /* we don't want to re-emit any escape string warnings */
  yyextra.escape_string_warning = false;

  /* Search for each constant, in sequence */
  for (i = 0; i < jstate->clocations_count; i++) {
    int loc = locs[i].location;
    int tok;

    /* Adjust recorded location if we're dealing with partial string */
    loc -= query_loc;

    pgqpAssert(loc >= 0);

    if (loc <= last_loc)
      continue; /* Duplicate constant, ignore */

    /* Lex tokens until we find the desired constant */
    for (;;) {
      tok = core_yylex(&yylval, &yylloc, yyscanner);

      /* We should not hit end-of-string, but if we do, behave sanely */
      if (tok == 0)
        break; /* out of inner for-loop */

      /*
       * We should find the token position exactly, but if we somehow
       * run past it, work with that.
       */
      if (yylloc >= loc) {
        if (query[loc] == '-') {
          /*
           * It's a negative value - this is the one and only case
           * where we replace more than a single token.
           *
           * Do not compensate for the core system's special-case
           * adjustment of location to that of the leading '-'
           * operator in the event of a negative constant.  It is
           * also useful for our purposes to start from the minus
           * symbol.  In this way, queries like "select * from foo
           * where bar = 1" and "select * from foo where bar = -2"
           * will have identical normalized query strings.
           */
          tok = core_yylex(&yylval, &yylloc, yyscanner);
          if (tok == 0)
            break; /* out of inner for-loop */
        }

        /*
         * We now rely on the assumption that flex has placed a zero
         * byte after the text of the current token in scanbuf.
         */
        locs[i].length = strlen(yyextra.scanbuf + loc);
        break; /* out of inner for-loop */
      }
    }

    /* If we hit end-of-string, give up, leaving remaining lengths -1 */
    if (tok == 0)
      break;

    last_loc = loc;
  }

  scanner_finish(yyscanner);
}

/*
 * pgqp_comp_location: comparator for qsorting pgqpLocationLen structs by location
 */
int pgqp_comp_location(const void *a, const void *b) {
  int l = ((const pgqpLocationLen *)a)->location;
  int r = ((const pgqpLocationLen *)b)->location;

  if (l < r)
    return -1;
  else if (l > r)
    return +1;
  else
    return 0;
}

#else
char *pgqp_gen_normquery(JumbleState *jstate, const char *query, int query_loc,
                    int *query_len_p) {
  char *norm_query;
  int query_len = *query_len_p;
  int i, norm_query_buflen, /* Space allowed for norm_query */
      len_to_wrt,           /* Length (in bytes) to write */
      quer_loc = 0,         /* Source query byte location */
      n_quer_loc = 0,       /* Normalized query byte location */
      last_off = 0,         /* Offset from start for previous tok */
      last_tok_len = 0;     /* Length (in bytes) of that tok */

  /*
   * Get constants' lengths (core system only gives us locations).  Note
   * this also ensures the items are sorted by location.
   */
  pgqp_fill_in_constant_lengths(jstate, query, query_loc);

  /*
   * Allow for $n symbols to be longer than the constants they replace.
   * Constants must take at least one byte in text form, while a $n symbol
   * certainly isn't more than 11 bytes, even if n reaches INT_MAX.  We
   * could refine that limit based on the max value of n for the current
   * query, but it hardly seems worth any extra effort to do so.
   */
  norm_query_buflen = query_len + jstate->clocations_count * 10;

  /* Allocate result buffer */
  norm_query = palloc(norm_query_buflen + 1);

  for (i = 0; i < jstate->clocations_count; i++) {
    int off,     /* Offset from start for cur tok */
        tok_len; /* Length (in bytes) of that tok */

    off = jstate->clocations[i].location;
    /* Adjust recorded location if we're dealing with partial string */
    off -= query_loc;

    tok_len = jstate->clocations[i].length;

    if (tok_len < 0)
      continue; /* ignore any duplicates */

    /* Copy next chunk (what precedes the next constant) */
    len_to_wrt = off - last_off;
    len_to_wrt -= last_tok_len;

    pgqpAssert(len_to_wrt >= 0);
    memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
    n_quer_loc += len_to_wrt;

    /* And insert a param symbol in place of the constant token */
    n_quer_loc += sprintf(norm_query + n_quer_loc, "$%d",
                          i + 1 + jstate->highest_extern_param_id);

    quer_loc = off + tok_len;
    last_off = off;
    last_tok_len = tok_len;
  }

  /*
   * We've copied up until the last ignorable constant.  Copy over the
   * remaining bytes of the original query string.
   */
  len_to_wrt = query_len - quer_loc;

  pgqpAssert(len_to_wrt >= 0);
  memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
  n_quer_loc += len_to_wrt;

  pgqpAssert(n_quer_loc <= norm_query_buflen);
  norm_query[n_quer_loc] = '\0';

  *query_len_p = n_quer_loc;
  return norm_query;
};

/*
 * Given a valid SQL string and an array of constant-location records,
 * fill in the textual lengths of those constants.
 *
 * The constants may use any allowed constant syntax, such as float literals,
 * bit-strings, single-quoted strings and dollar-quoted strings.  This is
 * accomplished by using the public API for the core scanner.
 *
 * It is the caller's job to ensure that the string is a valid SQL statement
 * with constants at the indicated locations.  Since in practice the string
 * has already been parsed, and the locations that the caller provides will
 * have originated from within the authoritative parser, this should not be
 * a problem.
 *
 * Duplicate constant pointers are possible, and will have their lengths
 * marked as '-1', so that they are later ignored.  (Actually, we assume the
 * lengths were initialized as -1 to start with, and don't change them here.)
 *
 * If query_loc > 0, then "query" has been advanced by that much compared to
 * the original string start, so we need to translate the provided locations
 * to compensate.  (This lets us avoid re-scanning statements before the one
 * of interest, so it's worth doing.)
 *
 * N.B. There is an assumption that a '-' character at a Const location begins
 * a negative numeric constant.  This precludes there ever being another
 * reason for a constant to start with a '-'.
 */
void pgqp_fill_in_constant_lengths(JumbleState *jstate, const char *query,
                              int query_loc) {
  LocationLen *locs;
  core_yyscan_t yyscanner;
  core_yy_extra_type yyextra;
  core_YYSTYPE yylval;
  YYLTYPE yylloc;
  int last_loc = -1;
  int i;

  /*
   * Sort the records by location so that we can process them in order while
   * scanning the query text.
   */
  if (jstate->clocations_count > 1)
    qsort(jstate->clocations, jstate->clocations_count, sizeof(LocationLen),
          pgqp_comp_location);
  locs = jstate->clocations;

  /* initialize the flex scanner --- should match raw_parser() */
  yyscanner = scanner_init(query, &yyextra, &ScanKeywords, ScanKeywordTokens);

  /* we don't want to re-emit any escape string warnings */
  yyextra.escape_string_warning = false;

  /* Search for each constant, in sequence */
  for (i = 0; i < jstate->clocations_count; i++) {
    int loc = locs[i].location;
    int tok;

    /* Adjust recorded location if we're dealing with partial string */
    loc -= query_loc;

    pgqpAssert(loc >= 0);

    if (loc <= last_loc)
      continue; /* Duplicate constant, ignore */

    /* Lex tokens until we find the desired constant */
    for (;;) {
      tok = core_yylex(&yylval, &yylloc, yyscanner);

      /* We should not hit end-of-string, but if we do, behave sanely */
      if (tok == 0)
        break; /* out of inner for-loop */

      /*
       * We should find the token position exactly, but if we somehow
       * run past it, work with that.
       */
      if (yylloc >= loc) {
        if (query[loc] == '-') {
          /*
           * It's a negative value - this is the one and only case
           * where we replace more than a single token.
           *
           * Do not compensate for the core system's special-case
           * adjustment of location to that of the leading '-'
           * operator in the event of a negative constant.  It is
           * also useful for our purposes to start from the minus
           * symbol.  In this way, queries like "select * from foo
           * where bar = 1" and "select * from foo where bar = -2"
           * will have identical normalized query strings.
           */
          tok = core_yylex(&yylval, &yylloc, yyscanner);
          if (tok == 0)
            break; /* out of inner for-loop */
        }

        /*
         * We now rely on the assumption that flex has placed a zero
         * byte after the text of the current token in scanbuf.
         */
        locs[i].length = strlen(yyextra.scanbuf + loc);
        break; /* out of inner for-loop */
      }
    }

    /* If we hit end-of-string, give up, leaving remaining lengths -1 */
    if (tok == 0)
      break;

    last_loc = loc;
  }

  scanner_finish(yyscanner);
};

/*
 * pgqp_comp_location: comparator for qsorting LocationLen structs by location
 */
int pgqp_comp_location(const void *a, const void *b) {
  int l = ((const LocationLen *)a)->location;
  int r = ((const LocationLen *)b)->location;

  if (l < r)
    return -1;
  else if (l > r)
    return +1;
  else
    return 0;
}
#endif
