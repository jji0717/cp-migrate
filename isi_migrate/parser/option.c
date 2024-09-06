/**
 * @file option.c
 * Copyright (c) 2006
 * Isilon Systems, Inc.  All rights reserved.
 */

/*-
 * Copyright (c) 1990, 1993, 1994
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

#include <err.h>
#include <fts.h>
#include <regex.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "siq_parser_private.h"

int typecompare(const void *, const void *);

/* NB: the following table must be sorted lexically. */
static OPTION const options[] = {
	{ "!",		c_simple,	f_not,		0 },
	{ "(",		c_simple,	f_openparen,	0 },
	{ ")",		c_simple,	f_closeparen,	0 },	
	{ "-and",	c_simple,	NULL,		F_AND },
	{ "-anewer",	c_newer,	f_newer,	F_TIME_A | F_LESSTHAN },
	{ "-aolder",	c_newer,	f_newer,	F_TIME_A | F_GREATER },
	{ "-atime",	c_newer,	f_newer,	F_TIME_A | F_EQUAL },	
	
	{ "-bnewer",	c_newer,	f_newer,	F_TIME_B | F_LESSTHAN },
	{ "-bolder",	c_newer,	f_newer,	F_TIME_B | F_GREATER },
	{ "-btime",	c_newer,	f_newer,	F_TIME_B | F_EQUAL },	
	
	{ "-cnewer",	c_newer,	f_newer,	F_TIME_C | F_LESSTHAN },
	{ "-colder",	c_newer,	f_newer,	F_TIME_C | F_GREATER },
	{ "-ctime",	c_newer,	f_newer,	F_TIME_C | F_EQUAL },	
	{ "-group",	c_group,	f_group,	0 },
	{ "-groupid",	c_groupid,	f_group,	0 },
	
	{ "-name",	c_name,		f_name,		0 },
	{ "-nogroup",	c_nogroup,	f_nogroup,	0 },
	{ "-nouser",	c_nouser,	f_nouser,	0 },
	{ "-or",	c_simple,	f_or,		0 },
	{ "-path", 	c_name,		f_path,		F_PATH },	
	{ "-regex",	c_regex,	f_regex,	0 },
	{ "-size",	c_size,		f_size,		0 },
	{ "-type",	c_type,		f_type,		0 },
	{ "-user",	c_user,		f_user,		0 },
	{ "-userid",	c_userid,	f_user,		0 }
};

/*
 * find_create --
 *	create a node corresponding to a command line argument.
 *
 * TODO:
 *	add create/process function pointers to node, so we can skip
 *	this switch stuff.
 */
siq_plan_t *
find_create(char ***argvp)
{
	OPTION *p;
	siq_plan_t *new;
	char **argv;

	argv = *argvp;

	if ((p = lookup_option(*argv)) == NULL) {		
		return NULL;
	}
	++argv;

	new = (p->create)(p, &argv);
	*argvp = argv;
	return (new);
}

OPTION *
lookup_option(const char *name)
{
	OPTION tmp;

	tmp.name = name;
	return ((OPTION *)bsearch(&tmp, options,
	    sizeof(options)/sizeof(OPTION), sizeof(OPTION), typecompare));
}

int
typecompare(const void *a, const void *b)
{
	return (strcmp(((const OPTION *)a)->name, ((const OPTION *)b)->name));
}


