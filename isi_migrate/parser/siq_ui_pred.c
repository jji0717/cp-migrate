/**
 * @file siq_pred_ui.c
 * Copyright (c) 2006
 * Isilon Systems, Inc.  All rights reserved.
 */
#include "siq_ui_pred_private.h"

int 
siq_ui_pred_from_plan(siq_ui_pred_t** pred, siq_plan_t* plan) 
{
	*pred = malloc(sizeof(siq_ui_pred_conjunct_t));
	if (*pred == NULL)
	{
		// ERROR REPORT: alloc. 
		return 2;
	}
	**pred = (siq_ui_pred_t)TAILQ_HEAD_INITIALIZER(**pred);
	TAILQ_INIT(*pred);
	return traverse_conj(plan, *pred);
}

static siq_ui_pred_literal_op_t
flags2op (const int flags, const int truth) 
{
	switch (flags & F_ELG_MASK) {
		case F_LESSTHAN:
			return truth ? SIQ_POF_LT : SIQ_POF_GE;
			break;
		case F_GREATER:
			return truth ? SIQ_POF_GT : SIQ_POF_LE;
			break;
		default:
			return truth ? SIQ_POF_EQ : SIQ_POF_NE;
	}
}

static siq_plan_t *
yanknode(siq_plan_t **planp)
{
	siq_plan_t *node;

	if ((node = (*planp)) == NULL)
		return (NULL);
	(*planp) = (*planp)->next;
	return (node);
}


static int
traverse_conj (siq_plan_t* plan, siq_ui_pred_t* pred) 
{
	int res = 0;
	siq_ui_pred_conjunct_t *conjunct = NULL;	
	if (plan == NULL) {
		// ERROR REPORT: plan is empty. 
		return 2;
	}	
		
	if ((plan->next == NULL) && (plan->execute == f_expr)) {
		res = traverse_conj(plan->p_data[0], pred);
	} else if ((plan->next == NULL) && (plan->execute == f_or)) {
		res = traverse_conj(plan->p_data[0], pred);
		if (res == 0)
			res = traverse_conj(plan->p_data[1], pred);
	} else {
		conjunct = malloc(sizeof(siq_ui_pred_conjunct_t));
		if (conjunct == NULL) {
			// ERROR REPORT: alloc.
			return 2;
		}
		conjunct->literals = (siq_ui_pred_literals_t) 
				TAILQ_HEAD_INITIALIZER(conjunct->literals);
		TAILQ_INIT(&conjunct->literals);
		res = traverse_lits(plan, conjunct, TRUE);
		if (res != 0)
			return res;
		TAILQ_INSERT_TAIL(pred, conjunct, conjuncts);
	}
	return res;
}

static int
traverse_lits(siq_plan_t* plan, siq_ui_pred_conjunct_t* conj, int truth) 
{
	int res = 0;
	siq_plan_t *next;
	siq_plan_t *head = plan;
	siq_ui_pred_literal_t *literal;	
	while (((next = yanknode(&head)) != NULL) && (res == 0)) {
		if (next->execute == f_expr) {
			res = traverse_lits(next->p_data[0], conj, truth);
		
		} else if (next->execute == f_not) {
			res = traverse_lits(next->p_data[0], conj, !truth);
		
		} else if (next->execute == f_or) {
			// ERROR REPORT: unsupported predicate format.
			return 1;
		
		} else {
			literal = calloc(1, sizeof(siq_ui_pred_literal_t));
			if (literal == NULL) {
				// ERROR REPORT: alloc.
				return 2;
			}
			TAILQ_INSERT_TAIL(&conj->literals,
				literal, literals);
			literal->cmp_op = flags2op(next->flags, truth);
			if (next->execute == f_regex) {
				literal->type = SIQ_POT_REGEX;
				literal->value = 
					strdup(next->a_data);
			} else if (next->execute == f_name) {
				literal->type = SIQ_POT_NAME;
				literal->value = 
					strdup(next->a_data);	
			} else if (next->execute == f_newer) {
				switch (next->flags & F_TIME_MASK) {
					case F_TIME_A:
						literal->type = SIQ_POT_ATIME;
						break;
					case F_TIME_B:
						literal->type = SIQ_POT_BTIME;
						break;
					case F_TIME_C:
						literal->type = SIQ_POT_CTIME;
						break;
					default:
						literal->type = SIQ_POT_NONE;
						break;
				}
				literal->value = strdup(next->a_data);
				
			} else if (next->execute == f_path) {
				literal->type = SIQ_POT_PATH;
				literal->value = strdup(next->a_data);
				
			} else if (next->execute == f_type) {
				literal->type = SIQ_POT_TYPE;
				switch (next->m_data) {
					case S_IFDIR:
						literal->value = strdup("d");
						break;
					case S_IFREG:
						literal->value = strdup("f");
						break;
					case S_IFLNK:
						literal->value = strdup("l");
						break;
					default:
						///errx(1, "unknown type: %d", 
						////next->m_data);
						return 1;
				}
				
			} else if (next->execute == f_size) {
				literal->type = SIQ_POT_SIZE;
				if ((next->a_data[0] == '+') ||
	 			    (next->a_data[0] == '-')) {
					literal->value = 
					   strdup(next->a_data+1);
				   }
				else {
					literal->value = 
						strdup(next->a_data);
				}
			} else if (next->execute == f_user) {
				if (next->a_data != NULL) {
					literal->type = SIQ_POT_USER;
					literal->value = strdup(next->a_data);
				}
				else {
					literal->type = SIQ_POT_USERID;
					literal->value = 
						(char*) malloc(
							sizeof(char)*INTLEN);
					sprintf(literal->value, "%d", 
							next->u_data); 
				}
			} else if (next->execute == f_group) {
				if (next->a_data != NULL) {
					literal->type = SIQ_POT_GROUP;
					literal->value = strdup(next->a_data);
				}
				else {
					literal->type = SIQ_POT_GROUPID;
					literal->value = 
						(char*) malloc(
							sizeof(char)*INTLEN);
					sprintf(literal->value, "%d", 
							next->g_data); 
				}
			} else if (next->execute == f_nouser) {
				literal->type = SIQ_POT_NOUSER;
				literal->value = NULL;
			} else if (next->execute == f_nogroup) {
				literal->type = SIQ_POT_NOGROUP;
				literal->value = NULL; 
			} else {
				// ERROR REPORT: uknown node type.
				return 1;	
			}
			if ((literal->value == NULL) && (next->execute != f_nouser)
		       	   && (next->execute != f_nogroup)) {
				// ERROR REPORT: alloc.
				return 2;
			}
		}
	}
	return res;
}

void siq_ui_pred_free(siq_ui_pred_t* pred) {
       siq_ui_pred_conjunct_t *conjunct, *conjunct_next;
       siq_ui_pred_literal_t *literal, *literal_next;

       conjunct = TAILQ_FIRST(pred);
       while(conjunct) {
              conjunct_next = TAILQ_NEXT(conjunct, conjuncts);
              literal = TAILQ_FIRST(&conjunct->literals);
              while(literal) {
                     literal_next = TAILQ_NEXT(literal, literals);
                     free(literal->value);
                     free(literal);
                     literal = literal_next;
              }
              free(conjunct);
              conjunct = conjunct_next;
       }
}
