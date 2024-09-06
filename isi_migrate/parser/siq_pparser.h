/**
 * @file siq_pparser.h
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
 *
 */
 
#ifndef _PPARSER_H_
#define _PPARSER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>
#include <regex.h>
#include <fts.h>
#include <stdbool.h>

/* function modifiers */
#define F_TIME_A	0x00000001	/* one of -atime, -anewer, -aolder */
#define	F_TIME_B	0x00000002	/* one of -btime, -bnewer, -bolder */
#define F_TIME_C	0x00000004	/* one of -ctime, -cnewer, -colder */
#define F_TIME_MASK 	0x00000007 
#define F_PATH		0x00000008
/* command line function modifiers */
#define	F_EQUAL		0x00000000	/* [acm]min [acm]time inum links size */
#define	F_LESSTHAN	0x00000100
#define	F_GREATER	0x00000200
#define F_ELG_MASK	0x00000300
#define	F_IGNCASE	0x00010000	/* iname ipath iregex */
#define F_AND		0x00020000	/* and */
#define	F_EXACTTIME	F_IGNCASE	/* -[acm]time units syntax */

/* forward declarations */
struct _siq_plan_t;
struct _select_entry_t;

/* execute function */
typedef int siq_exec_f(struct _siq_plan_t *, struct _select_entry_t *);

/* node definition */
typedef struct _siq_plan_t {
	struct _siq_plan_t *next;		/**< next node */
	siq_exec_f	*execute;		/**< node evaluation function */
	int flags;				/**< private flags */
	union {
		mode_t _m_data;			/**< mode mask */
		gid_t _g_data;			/**< gid */
		uid_t _u_data;			/**< uid */
		long long _o_data;		/**< file size */
		struct _siq_plan_t *_p_data[2];	/**< PLAN trees */
		regex_t* _re_data;		/**< regex */
	} p_un;
	char *a_data;				/**< original value */
	bool need_fname;
	bool need_dname;
	bool need_size;
	bool need_date;
	bool need_type;
	bool need_path;
} siq_plan_t;

typedef struct _select_entry_t {
	char *name;
	char *path;
	struct stat *st;
	unsigned char type;
} select_entry_t;

__BEGIN_DECLS

/**
 * Parses a literal @predicate into an expression tree.
 * @time is used for date expressions checking.
 * @Returns a pointer to the expression tree, to be provided 
 * on further calls to parser_execute, on success, and NULL otherwise.
 */
siq_plan_t *
siq_parser_parse(char *predicate, time_t time);

/**
 * Reconstructs literal predicate representation for an expression tree
 * given by @plan. 
 * @Returns a pointer to the literal representation.
 */ 
char *
siq_parser_unparse(siq_plan_t *plan);

/**
 * Executes a previously parsed @plan on (@path, @name, @stat) at 
 * @time instant.
 * @Returns 0 if given file information doesn`t satisfy @plan, otherwise 
 * returs non-zero value.
 */
int
siq_parser_execute(siq_plan_t *plan, select_entry_t *entry, time_t time);

/**
 * Correctly frees @plan structure.
 */ 
void
siq_plan_free(siq_plan_t *plan);

struct timeb;

/**
 * @Returns timestamp is calculated by addition @since and interval in @pattern
 * Pattern examples: 
 *             1 month ago
 *             2 hours ago
 *             400000 seconds ago
 *             last year
 *             last Monday
 *             yesterday
 *             a fortnight ago
 *             3/31/92 10:00:07 PST
 *             January 23, 1987 10:05pm
 *             22:00 GMT
 * @tb is depricated, use NULL. 
 */
time_t          
siq_get_date(char *pattern, time_t since);

__END_DECLS

#ifdef __cplusplus
}
#endif

#endif
