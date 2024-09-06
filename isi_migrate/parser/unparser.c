/**
 * @file unparser.c
 * Copyright (c) 2006
 * Isilon Systems, Inc.  All rights reserved.
 */

#include <sys/cdefs.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <err.h>
#include <errno.h>
#include <fts.h>
#include <regex.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "siq_parser_private.h"

#define PREDICATEMAXLEN 5000

static char res[PREDICATEMAXLEN];
static char tmp[PREDICATEMAXLEN];

char* unparse(siq_plan_t *);
char* do_and(siq_plan_t *);

char* 
do_and(siq_plan_t *plan) {
	siq_plan_t *p = plan;
	while (p) {
		unparse(p);		
		p = p->next;
		if (p) 
			strcat(res, " -and");
	}
	return res;
}

char *
unparse(siq_plan_t *plan) {	
	if (plan->execute == f_expr) {			// ()
		strcat(res, " (");
		do_and(plan->p_data[0]);
		strcat(res, " )");		
	} 
	else if (plan->execute == f_not) {		// NOT
		strcat(res, " !");
		do_and(plan->p_data[0]);
	} 
	else if (plan->execute == f_or) {		// OR
		do_and(plan->p_data[0]);
		strcat(res, " -or");
		do_and(plan->p_data[1]);
	} 
	else if (plan->execute == f_regex) {	// NAME	REGEX
		strcat(res, " -regex ");
		strcat(res, plan->a_data);
	} 
	else if (plan->execute == f_newer) {	// TIME				
		switch (plan->flags & F_TIME_MASK) {
			case F_TIME_A:
				strcat(res, " -a");
				break;
			case F_TIME_B:
				strcat(res, " -b");
				break;
			case F_TIME_C:
				strcat(res, " -c");
				break;				
		}		
		if (plan->flags & F_GREATER) {
			strcat(res, "older");
		}
		else if (plan->flags & F_LESSTHAN) {
			strcat(res, "newer");
		}
		else 
			strcat(res,"time");
		strcat(res, " '");
		strcat(res, plan->a_data);
		strcat(res, "'");
	} 
	else if (plan->execute == f_path) {		// PATH, NAME
		strcat(res, " -path ");
		strcat(res, plan->a_data);
	} 
	else if (plan->execute == f_name) {		/* NAME */
		strcat(res, " -name ");
		strcat(res, plan->a_data);
	} 	
	else if (plan->execute == f_type) {		/* TYPE	*/
		strcat(res, " -type");
		switch (plan->m_data) {
			case S_IFDIR:
				strcat(res, " d");
				break;
			case S_IFREG:
				strcat(res, " f");
				break;
			case S_IFLNK:
				strcat(res, " l");
				break;
			default:
				errx(1, "unknown type: %d", plan->m_data);
				break;
		}
	}
	else if (plan->execute == f_size) {		/* SIZE */
		strcat(res, " -size ");
		strcat(res, plan->a_data);
	}
	else if (plan->execute == f_user) {		/* USER */
		if (plan->a_data == NULL) {
			sprintf(tmp, "%d", plan->u_data);
			strcat(res, " -userid ");
			strcat(res, tmp);
		}
		else 
		{
			strcat(res, " -user ");
			strcat(res, plan->a_data);
		}
	}
	else if (plan->execute == f_group) {		/* GROUP */
		if (plan->a_data == NULL) {
			sprintf(tmp, "%d", plan->g_data);
			strcat(res, " -groupid ");
			strcat(res, tmp);
		}
		else 
		{
			strcat(res, " -group ");
			strcat(res, plan->a_data);
		}
	}
	else if (plan->execute == f_nogroup) {
		strcat(res, " -nogroup");
	}
	else if (plan->execute == f_nouser) {
		strcat(res, " -nouser");
	}

	return res;
}

char* 
siq_parser_unparse(siq_plan_t *plan)
{	
	res[0] = '\0';
	return do_and(plan)+1;
}
