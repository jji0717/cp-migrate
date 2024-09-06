/**
 * @file pparser.c
 * Copyright (c) 2006
 * Isilon Systems, Inc.  All rights reserved.
 */

/*-
 * Copyright (c) 1991, 1993, 1994
 *	The Regents of the University of California.  All rights reserved.
 *
 * This code is derived from software contributed to Berkeley by
 * Cimarron D. Taylor of the University of California, Berkeley.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *	This product includes software developed by the University of
 *	California, Berkeley and its contributors.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include <sys/cdefs.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <ctype.h>
#include <err.h>
#include <errno.h>
#include <fts.h>
#include <regex.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <isi_util/isi_assert.h>

#include "siq_parser_private.h"

#define INC_AP()\
	do {\
		cpredicate[i] = '\0';\
		if ((strlen(*ap) != 0) && (i+1 < len))\
			ap++;\
		if (i+1 < len) {\
			*ap = cpredicate+i+1;\
		}\
	} while (0)\

time_t pparser_now;		/* time of instant to evaluate a predicate at */
int custom_error = 0;

/*
 * parser_parse --
 *	process the predicate string and create a "plan". utf8 needed!
 */
siq_plan_t *
siq_parser_parse(char *predicate, time_t time)
{
	siq_plan_t *plan, *tail, *new;
	bool need_fname = false;
	bool need_size = false;
	bool need_path = false;
	bool need_date = false;
	bool need_dname = false;
	bool need_type = false;

	char **ap, **argvf, **argv;
	char *cpredicate, *tmp;
	size_t len;
	size_t i;
	int state;
	bool parse_error = false;

	plan = NULL;
	cpredicate = NULL;
	argvf = NULL;

	pparser_now = time;
	if (predicate == NULL)
		goto out;
	cpredicate = strdup(predicate);
	if (cpredicate == NULL)
		goto out;
	i = strlen(cpredicate)-1;
	for (tmp = cpredicate + i; tmp >= cpredicate && isspace(*tmp); tmp--);
	tmp[1] = '\0';
	len = strlen(cpredicate);
	// predicate is all whitespace
	if (len	== 0) {
		goto out;
	}
	argvf = (char**) calloc(len, sizeof(char*));
	ASSERT(argvf != NULL);

	state = 0;
	ap = argvf;
	*ap = cpredicate;
	for (i=0; i < len; i++) {
		switch (state) {
			case 0:
				if ((cpredicate[i] == ' ')
					|| (cpredicate[i] == '\t'))
				{
					INC_AP();
				}
				else if (cpredicate[i] == '\'')
				{
					state = 1;
					INC_AP();
				}
				break;
			case 1:
				if (cpredicate[i] == '\'')
				{
					state = 0;
					INC_AP();
				}
				break;
		}
	}

	/* Unmatched \' in predicate */
	if (state == 1) {
		goto out;
	}

	ap++;
	*ap = NULL;
	argv = argvf;

	/*
	 * for each argument in the command line, determine what kind of node
	 * it is, create the appropriate node type and add the new plan node
	 * to the end of the existing plan.  The resulting plan is a linked
	 * list of plan nodes.  For example, the string:
	 *
	 *	% find . -name foo -newer bar -print
	 *
	 * results in the plan:
	 *
	 *	[-name foo]--> [-newer bar]--> [-print]
	 *
	 * in this diagram, `[-name foo]' represents the plan node generated
	 * by c_name() with an argument of foo and `-->' represents the
	 * plan->next pointer.
	 */
	for (plan = tail = NULL; *argv;) {
		if (!(new = find_create(&argv))) {
			parse_error = true;
			goto out;
		}

		if ((new->flags & F_AND) == F_AND) {
			free(new);
			continue;
		}

		if (plan == NULL)
			tail = plan = new;
		else {
			tail->next = new;
			tail = new;
		}

		/* track data required to evaluate in the plan root */
		if (new->execute == f_name)
			need_fname = true;
		else if (new->execute == f_size)
			need_size = true;
		else if (new->execute == f_path)
			need_path = true;
		/* needed for default selection criteria, see below
		else if (new->execute == f_type)
			plan->need_type = true; */
		else if (new->execute == f_newer)
			need_date = true;
	}

	/* data needed for default selection criteria */
	need_dname = true;
	need_type = true;

	/*
	 * the command line has been completely processed into a search plan
	 * except for the (, ), !, and -o operators.  Rearrange the plan so
	 * that the portions of the plan which are affected by the operators
	 * are moved into operator nodes themselves.  For example:
	 *
	 *	[!]--> [-name foo]--> [-print]
	 *
	 * becomes
	 *
	 *	[! [-name foo] ]--> [-print]
	 *
	 * and
	 *
	 *	[(]--> [-depth]--> [-name foo]--> [)]--> [-print]
	 *
	 * becomes
	 *
	 *	[expr [-depth]-->[-name foo] ]--> [-print]
	 *
	 * operators are handled in order of precedence.
	 */

	plan = paren_squish(plan);		/* ()'s */
	plan = not_squish(plan);		/* !'s */
	plan = or_squish(plan);			/* -o's */
	if (custom_error != 0) {
		custom_error = 0;
		parse_error = true;
		goto out;
	}
	
	if (plan) {
		plan->need_fname = need_fname;
		plan->need_path = need_path;
		plan->need_size = need_size;
		plan->need_date = need_date;
		plan->need_dname = need_dname;
		plan->need_type = need_type;
	}
 
out:
	if (cpredicate != NULL)
		free(cpredicate);
	if (argvf != NULL)
		free(argvf);

	if (parse_error) {
		if (plan != NULL) {
			siq_plan_free(plan);
		}
		return NULL;
	} else {
		return (plan);
	}
}

/*
 * parser_execute --
 *	executes the plan	
 */
int
siq_parser_execute(siq_plan_t *plan, select_entry_t *entry, time_t t)
{
	siq_plan_t *p;
	int rval = 0;
	pparser_now = t;

	for (p = plan; p && (rval = (p->execute)(p, entry)); p = p->next);

	return (rval);
}
