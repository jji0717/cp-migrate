/**
 * @file siq_pred_ui.h
 * Copyright (c) 2006
 * Isilon Systems, Inc.  All rights reserved.
 */

#ifndef __SIQ_PRED_UI_H__
#define __SIQ_PRED_UI_H__

#include <stdio.h>
#include <stdlib.h>
#include <sys/queue.h>
#include "siq_pparser.h"

/* Predicate operation types */
typedef enum siq_ui_pred_literal_type {
       SIQ_POT_NONE=0,
       SIQ_POT_ATIME,
       SIQ_POT_BTIME,
       SIQ_POT_CTIME,       
       SIQ_POT_NAME,
       SIQ_POT_PATH,
       SIQ_POT_REGEX,
       SIQ_POT_SIZE,
       SIQ_POT_TYPE,
       SIQ_POT_USER,
       SIQ_POT_USERID,
       SIQ_POT_GROUP,
       SIQ_POT_GROUPID,
       SIQ_POT_NOUSER,
       SIQ_POT_NOGROUP,
       SIQ_POT_TOTAL
} siq_ui_pred_literal_type_t;

extern const char *siq_ui_pred_literal_type_strs[SIQ_POT_TOTAL+1];

/* Predicate operation flags */
typedef enum siq_ui_pred_literal_op {
       SIQ_POF_EQ=0,
       SIQ_POF_NE,
       SIQ_POF_GT,
       SIQ_POF_LT,
       SIQ_POF_LE,
       SIQ_POF_GE,
       SIQ_POF_TOTAL
} siq_ui_pred_literal_op_t;

extern const char *siq_ui_pred_literal_op_strs[SIQ_POF_TOTAL+1];

/* DATA STRUCTURES FOR DNF */

/* A conjunctive list of literals in DNF */
typedef struct siq_ui_pred_literal {
       siq_ui_pred_literal_type_t type;
       siq_ui_pred_literal_op_t cmp_op;
       char *value;
       TAILQ_ENTRY(siq_ui_pred_literal) literals;
} siq_ui_pred_literal_t;
typedef TAILQ_HEAD(siq_ui_pred_literals, siq_ui_pred_literal)
                siq_ui_pred_literals_t;

/* A disjunctive list of conjuncts in DNF */
typedef struct siq_ui_pred_conjunct {
       siq_ui_pred_literals_t literals;
       TAILQ_ENTRY(siq_ui_pred_conjunct) conjuncts;
} siq_ui_pred_conjunct_t;
typedef TAILQ_HEAD(siq_ui_pred, siq_ui_pred_conjunct) siq_ui_pred_t;

/**
* Transforms predicate plan representation to the UI form.
* @input:
* plan - valid siq_plan_t structure
*
* @output:
* pred - pointer to siq_ui_pred_t structure
          containing predicate in DNF form.
* @returns
* 0 on success, and non-zero error code on failure.
*/
int siq_ui_pred_from_plan(siq_ui_pred_t** pred, siq_plan_t* plan);
//siq_ui_pred_t siq_ui_pred_to_str(siq_plan_t);
void siq_ui_pred_free(siq_ui_pred_t*);
//void siq_ui_pred_print(siq_ui_pred_t);

#endif
