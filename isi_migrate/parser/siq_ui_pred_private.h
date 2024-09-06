#ifndef SIQ_UI_PRED_PRIVATE_H_
#define SIQ_UI_PRED_PRIVATE_H_

#include <sys/queue.h>
#include <string.h>
#include "siq_parser_private.h"
#include "siq_ui_pred.h"

#undef  TRUE
#define TRUE    1

#undef  FALSE
#define FALSE   0

#define INTLEN	15

const char *siq_ui_pred_literal_type_strs[SIQ_POT_TOTAL+1] = {
       "none",
       "atime",
       "btime",
       "ctime",       
       "name",
       "path",
       "regex",
       "size",
       "type",
       "user",
       "userid",
       "group",
       "groupid",
       "nouser",
       "nogroup",
       NULL
};

const char *siq_ui_pred_literal_op_strs[SIQ_POF_TOTAL+1] = {
       "==",
       "!=",
       ">",
       "<",
       "<=",
       ">=",
       NULL
};

static int traverse_conj (siq_plan_t*, siq_ui_pred_t*);
static siq_ui_pred_literal_op_t	flags2op (const int, const int);
static int traverse_lits(siq_plan_t*, siq_ui_pred_conjunct_t*, int truth);

#endif /*SIQ_UI_PRED_PRIVATE_H_*/
