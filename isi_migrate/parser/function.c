/**
 * @file function.c
 * Copyright (c) 2006
 * Isilon Systems, Inc.  All rights reserved.
 */

/*-
 * Copyright (c) 1990, 1993
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
#include <limits.h>
#include <sys/cdefs.h>
#include <sys/param.h>
#include <sys/ucred.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/acl.h>
#include <sys/wait.h>
#include <sys/mount.h>

#include <dirent.h>
#include <err.h>
#include <pwd.h>
#include <errno.h>
#include <fnmatch.h>
#include <fts.h>
#include <grp.h>
#include <limits.h>
#include <pwd.h>
#include <regex.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <stdio.h>

#include "siq_parser_private.h"

static siq_plan_t *palloc(OPTION *);
static long long find_parsenum(siq_plan_t *, const char *, char *, char *);
static char *nextarg(OPTION *, char ***);
int regexp_flags = REG_EXTENDED;

extern char **environ;

#define	COMPARE(a, b) do {						\
	switch (plan->flags & F_ELG_MASK) {				\
	case F_EQUAL:							\
		return (a == b);					\
	case F_LESSTHAN:						\
		return (a < b);						\
	case F_GREATER:							\
		return (a > b);						\
	default:							\
		abort();						\
	}								\
} while(0)

char * 
strclone(char *src) 
{
	size_t len = strlen(src);
	char * dst;
	if ((dst = (char *) malloc(sizeof(char)*(len+1))) == NULL) {
		return NULL;
	}
	strcpy(dst, src);
	return dst;
}

static siq_plan_t *
palloc(OPTION *option)
{
	siq_plan_t *new;

	new = calloc(1, sizeof(siq_plan_t));
	if (new == NULL) {
		return NULL;	
	}
	new->execute = option->execute;
	new->flags = option->flags;
	return new;
}

/*
 * find_parsenum --
 *	Parse a string of the form [+-]# and return the value.
 *  if an error is occured find_parsenum returns LLONG_MAX
 */
static long long
find_parsenum(siq_plan_t *plan, const char *option, char *vp, char *endch)
{
	long long value;
	char *endchar, *str;	/* Pointer to character ending conversion. */

	/* Determine comparison from leading + or -. */
	str = vp;
	switch (*str) {
	case '+':
		++str;
		plan->flags |= F_GREATER;
		break;
	case '-':
		++str;
		plan->flags |= F_LESSTHAN;
		break;
	default:
		plan->flags |= F_EQUAL;
		break;
	}

	/*
	 * Convert the string with strtoq().  Note, if strtoq() returns zero
	 * and endchar points to the beginning of the string we know we have
	 * a syntax error.
	 */
	value = strtoq(str, &endchar, 10);
	if (value == 0 && endchar == str) {
		return LLONG_MAX;
	}
	if (endchar[0] && endch == NULL)
	{
		return LLONG_MAX;
	}
	if (endch)
		*endch = endchar[0];
	return value;
}

/*
 * nextarg --
 *	Check that another argument still exists, return a pointer to it,
 *	and increment the argument vector pointer.
 */
static char *
nextarg(OPTION *option, char ***argvp)
{
	char *arg;

	if ((arg = **argvp) == 0) {
		return NULL;
	}
	(*argvp)++;
	return arg;
} /* nextarg() */

siq_plan_t *
c_name(OPTION *option, char ***argvp)
{
	char *pattern;
	siq_plan_t *new;

	pattern = nextarg(option, argvp);
	if (pattern == NULL)
		return NULL;
	new = palloc(option);
	if (new == NULL)
		return NULL;
	new->a_data = strclone(pattern);
	if (new->a_data == NULL) {
		siq_plan_free(new);
		return NULL;
	}
	return new;
}

/*
 * -newer/older/time functions --
 *
 *	True if the current entry has been modified more recently
 *	then the modification time of the file named by the pathname
 *	file.
 */
int
f_newer(siq_plan_t *plan, select_entry_t *entry)
{
	time_t tdata = siq_get_date(plan->a_data, pparser_now);
	time_t tm = -1;

	if (plan->flags & F_TIME_C)
		tm = entry->st->st_ctime;
	else if (plan->flags & F_TIME_A)
		tm = entry->st->st_atime;
	else if (plan->flags & F_TIME_B)
		tm = entry->st->st_birthtime;
	if (plan->flags & F_LESSTHAN) 
		return (tm > tdata) && (tm <= pparser_now);
	else if (plan->flags & F_GREATER) 
		return (tm < tdata) && (tm <= pparser_now);		
	else 
		return tm == tdata;		
}

siq_plan_t *
c_newer(OPTION *option, char ***argvp)
{
	char *fn_or_tspec;
	siq_plan_t *new;

	fn_or_tspec = nextarg(option, argvp);
	if (fn_or_tspec == NULL) {
		return NULL;
	}
	
	if (strlen(fn_or_tspec) == 0) {
		return NULL;
	}

	new = palloc(option);
	if (new == NULL) {
		return NULL;
	}
		
	/* compare against what */
	time_t tdata = siq_get_date(fn_or_tspec, pparser_now);
	if (tdata == (time_t) -1) {
		siq_plan_free(new);
		return NULL;
	}
	new->a_data = strclone(fn_or_tspec);
	if (new->a_data == NULL) {
		siq_plan_free(new);
		return NULL;
	}	
	return new;
}

/*
 * -name functions --
 *
 *	True if the basename of the filename being examined
 *	matches pattern using Pattern Matching Notation S3.14
 */
int
f_name(siq_plan_t *plan, select_entry_t *entry)
{
	return !fnmatch(plan->a_data, entry->name, 0);
}

/*
 * -path functions --
 *
 *	True if the path of the filename being examined
 *	matches pattern using Pattern Matching Notation S3.14
 */
int
f_path(siq_plan_t *plan, select_entry_t *entry)
{
	return !fnmatch(plan->a_data, entry->path, 0);
}

/* c_path is the same as c_name */

/*
 * -regex functions --
 *
 *	True if the whole path of the file matches pattern using
 *	regular expression.
 */
int
f_regex(siq_plan_t *plan, select_entry_t *entry)
{
	char *str;
	int len;	
	regmatch_t pmatch;
	int errcode;
	char errbuf[LINE_MAX];
	int matched;
	
	str = entry->name;
	len = strlen(str);
	matched = 0;

	pmatch.rm_so = 0;
	pmatch.rm_eo = len;

	errcode = regexec(plan->re_data, str, 1, &pmatch, REG_STARTEND);

	if (errcode != 0 && errcode != REG_NOMATCH) {
		regerror(errcode, plan->re_data, errbuf, sizeof errbuf);
		return -1;
	}

	if (errcode == 0 && pmatch.rm_so == 0 && pmatch.rm_eo == len)
		matched = 1;

	return matched;
}

siq_plan_t *
c_regex(OPTION *option, char ***argvp)
{
	siq_plan_t *new;
	char *pattern;
	int errcode;
	char errbuf[LINE_MAX];

	new = palloc(option);
	if (new == NULL) {
		return NULL;
	}

	if ((new->re_data = 
	     (regex_t *) malloc(sizeof(regex_t))) == NULL) {
		siq_plan_free(new);
		return NULL;
	}
	
	pattern = nextarg(option, argvp);	
	if (pattern == NULL) {
		siq_plan_free(new);
		return NULL;
	}
	
	new->a_data = strclone(pattern);
	if (new->a_data == NULL) {
		siq_plan_free(new);
		return NULL;
	}

	if ((errcode = regcomp(new->re_data, new->a_data,
	     regexp_flags)) != 0) {	    	
		regerror(errcode, new->re_data, errbuf, sizeof errbuf);
		siq_plan_free(new);
		return NULL;
	}	
	
	return new;
}

/* c_simple covers c_prune, c_openparen, c_closeparen, c_not, c_or */

siq_plan_t *
c_simple(OPTION *option, char ***argvp __unused)
{
	return palloc(option);
}

/*
 * -size n[c] functions --
 *
 *	True if the file size in bytes, divided by an implementation defined
 *	value and rounded up to the next integer, is n.  If n is followed by
 *      one of c k M G T P, the size is in bytes, kilobytes,
 *      megabytes, gigabytes, terabytes or petabytes respectively.
 */
#define	FIND_SIZE	512

int
f_size(siq_plan_t *plan, select_entry_t *entry)
{
	long long size;

	size = entry->st->st_size;
	COMPARE(size, plan->o_data);
}

siq_plan_t *
c_size(OPTION *option, char ***argvp)
{
	char *size_str;
	siq_plan_t *new;
	char endch;
	long long scale;

	size_str = nextarg(option, argvp);
	if (size_str == NULL) {
		return NULL;
	}

	new = palloc(option);
	if (new == NULL) {
		return NULL;
	}
	endch = 'B';
	new->o_data = find_parsenum(new, option->name, size_str, &endch);
	if (new->o_data == LLONG_MAX) {
		siq_plan_free(new);
		return NULL;
	}
	
	new->a_data = strclone(size_str);
	if (new->a_data == NULL)
	{
		siq_plan_free(new);
		return NULL;
	}
	if (endch != '\0') {
		
		switch (endch) {
		case 'B':                       /* characters */
			scale = 0x1LL;
			break;
		case 'K':                       /* kilobytes 1<<10 */
			scale = 0x400LL;
			break;
		case 'M':                       /* megabytes 1<<20 */
			scale = 0x100000LL;
			break;
		case 'G':                       /* gigabytes 1<<30 */
			scale = 0x40000000LL;
			break;
		case 'T':                       /* terabytes 1<<40 */
			scale = 0x10000000000LL;
			break;
		case 'P':                       /* petabytes 1<<50 */
			scale = 0x4000000000000LL;
			break;
		case 'E':                       /* exabytes 1<<60 */
			scale = 0x1000000000000000LL;
			break;
		default:
			siq_plan_free(new);
			return NULL;			
		}
		/* Max file size = 8EB - 1 */
		if (new->o_data > QUAD_MAX / scale) {
			siq_plan_free(new);
			return NULL;
		}		
		new->o_data *= scale;
	}
	else
	{
		siq_plan_free(new);
		return NULL;
	}

	return new;
}

/*
 * -type c functions --
 *
 *	True if the type of the file is c, where c is b, c, d, p, f or w
 *	for block special file, character special file, directory, FIFO,
 *	regular file or whiteout respectively.
 */
int
f_type(siq_plan_t *plan, select_entry_t *entry)
{
	return (DTTOIF(entry->type) & S_IFMT) == plan->m_data;
}

siq_plan_t *
c_type(OPTION *option, char ***argvp)
{
	char *typestring;
	siq_plan_t *new;
	mode_t  mask;

	typestring = nextarg(option, argvp);
	if (typestring == NULL) {
		return NULL;
	}

	switch (typestring[0]) {
	case 'd':
		mask = S_IFDIR;
		break;
	case 'f':
		mask = S_IFREG;
		break;
	case 'l':
		mask = S_IFLNK;
		break;
	default:
		return NULL;
	}

	new = palloc(option);
	if (new == NULL) {
		return NULL;
	}
	new->m_data = mask;
	return new;
}

/*
 * -group gname functions --
 *
 *	True if the file belongs to the group gname.  If gname is numeric and
 *	an equivalent of the getgrnam() function does not return a valid group
 *	name, gname is taken as a group ID.
 */
int
f_group(siq_plan_t *plan, select_entry_t *entry)
{
     	return entry->st->st_gid == plan->g_data;
}

siq_plan_t *
c_group(OPTION *option, char ***argvp)
{
     	char *gname;
	siq_plan_t *new;
	struct group *g;
	gid_t gid;

	gname = nextarg(option, argvp);
	if (gname == NULL) {
		return NULL;
	}
	g = getgrnam(gname);
	if (g == NULL) {
		gid = atoi(gname);
	} else
		gid = g->gr_gid;
	new = palloc(option);
	if (new == NULL) {
		return NULL;
	}
	new->g_data = gid;
	new->a_data = strclone(gname);
	if (new->a_data == NULL) {
		siq_plan_free(new);
		return NULL;
	}
	return new;
}

siq_plan_t *
c_groupid(OPTION *option, char ***argvp)
{
     	char *groupid;
	siq_plan_t *new;
	gid_t gid;

	groupid = nextarg(option, argvp);
	if (groupid == NULL) {
		return NULL;
	}
	gid = atoi(groupid);
	if (gid == 0 && groupid[0] != '0') {
		return NULL;
	}

	new = palloc(option);
	if (new == NULL) {
		return NULL;
	}
	new->g_data = gid;
	new->a_data = NULL;
	return new;
}

/*
 * -user uname functions --
 *
 *	True if the file belongs to the user uname.  If uname is numeric and
 *	an equivalent of the getpwnam() S9.2.2 [POSIX.1] function does not
 *	return a valid user name, uname is taken as a user ID.
 */
int
f_user(siq_plan_t *plan, select_entry_t *entry)
{
    	return entry->st->st_uid == plan->u_data;
}

siq_plan_t *
c_user(OPTION *option, char ***argvp)
{
     	char *username;
	siq_plan_t *new;
	struct passwd *p;
	uid_t uid;

	username = nextarg(option, argvp);
	if (username == NULL) {
		return NULL;
	}

	p = getpwnam(username);
	if (p == NULL) {
		return NULL;
	} else
		uid = p->pw_uid;

	new = palloc(option);
	if (new == NULL) {
		return NULL;
	}
	new->u_data = uid;
	new->a_data = strclone(username);
	if (new->a_data == NULL) {
		siq_plan_free(new);
		return NULL;
	}
	return new;
}

siq_plan_t *
c_userid(OPTION *option, char ***argvp)
{
     	char *userid;
	siq_plan_t *new;
	uid_t uid;

	userid = nextarg(option, argvp);
	if (userid == NULL) {
		return NULL;
	}

	uid = atoi(userid);
	if (uid == 0 && userid[0] != '0') {
		return NULL;
	}

	new = palloc(option);
	if (new == NULL) {
		return NULL;
	}
	new->u_data = uid;
	new->a_data = NULL;
	return new;
}

/*
 * -nogroup functions --
 *
 *	True if file belongs to a user ID for which the equivalent
 *	of the getgrnam() 9.2.1 [POSIX.1] function returns NULL.
 */
int
f_nogroup(siq_plan_t *plan __unused, select_entry_t *entry)
{
  	return group_from_gid(entry->st->st_gid, 1) == NULL;
}

siq_plan_t *
c_nogroup(OPTION *option, char ***argvp __unused)
{
	return palloc(option);
}

/*
 * -nouser functions --
 *
 *	True if file belongs to a user ID for which the equivalent
 *	of the getpwuid() 9.2.2 [POSIX.1] function returns NULL.
 */
int
f_nouser(siq_plan_t *plan __unused, select_entry_t *entry)
{
	return user_from_uid(entry->st->st_uid, 1) == NULL;
}

siq_plan_t *
c_nouser(OPTION *option, char ***argvp __unused)
{
	return palloc(option);
}

/*
 * ( expression ) functions --
 *
 *	True if expression is true.
 */
int
f_expr(siq_plan_t *plan, select_entry_t *entry)
{
	siq_plan_t *p;
	int state = 0;

	for (p = plan->p_data[0];
	    p && (state = (p->execute)(p, entry)); p = p->next);
	return state;
}

/*
 * f_openparen and f_closeparen nodes are temporary place markers.  They are
 * eliminated during phase 2 of parse_parse() --- the '(' node is converted
 * to a f_expr node containing the expression and the ')' node is discarded.
 * The functions themselves are only used as constants.
 */

int
f_openparen(siq_plan_t *plan __unused, select_entry_t *entry __unused)
{
	abort();
}

int
f_closeparen(siq_plan_t *plan __unused, select_entry_t *entry __unused)
{
	abort();
}

/* c_openparen == c_simple */
/* c_closeparen == c_simple */

/*
 * ! expression functions --
 *
 *	Negation of a primary; the unary NOT operator.
 */
int
f_not(siq_plan_t *plan, select_entry_t *entry)
{
	siq_plan_t *p;
	int state = 0;

	for (p = plan->p_data[0];
	    p && (state = (p->execute)(p, entry)); p = p->next);
	return !state;
}

/* c_not == c_simple */

/*
 * expression -o expression functions --
 *
 *	Alternation of primaries; the OR operator.  The second expression is
 * not evaluated if the first expression is true.
 */
int
f_or(siq_plan_t *plan, select_entry_t *entry)
{
	siq_plan_t *p;
	int state = 0;

	for (p = plan->p_data[0];
	    p && (state = (p->execute)(p, entry)); p = p->next);

	if (state)
		return 1;

	for (p = plan->p_data[1];
	    p && (state = (p->execute)(p, entry)); p = p->next);
	return state;
}

/* c_or == c_simple */

void
siq_plan_free(siq_plan_t *plan) 
{
	if (plan == NULL)
		return;
	siq_plan_free(plan->next);
	if (plan->a_data != NULL) {
		free(plan->a_data);
	}
	if (plan->execute == f_expr) {			// ()
		siq_plan_free(plan->p_data[0]);
	} 
	else if (plan->execute == f_not) {		// NOT
		siq_plan_free(plan->p_data[0]);
	} 
	else if (plan->execute == f_or) {		// OR
		siq_plan_free(plan->p_data[1]);
		siq_plan_free(plan->p_data[0]);
	} 
	else if (plan->execute == f_regex) {	// REGEX
		regfree(plan->re_data);
		free(plan->re_data);
	} 
	else if (plan->execute == f_type) {		// TYPE		
		/* NOTHING to clean */
	}
	else if (plan->execute == f_size) {		// SIZE
	}
	else if (plan->execute == f_user) {
	}
	else if (plan->execute == f_group) {
	}
	free(plan);
}
