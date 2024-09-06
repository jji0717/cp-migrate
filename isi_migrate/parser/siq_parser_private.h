/**
 * @file siq_parser_private.h
 * Copyright (c) 2006
 * Isilon Systems, Inc.  All rights reserved.
 */

/*-
 * Copyright (c) 1991, 1993, 1994
 *	The Regents of the University of California.  All rights reserved.
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
#ifndef _SIQ_PARSER_PRIVATE_H_
#define _SIQ_PARSER_PRIVATE_H_

#include <sys/cdefs.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "siq_pparser.h"

#ifdef __cplusplus
extern "C" {
#endif

struct _option;
/* create function */
typedef	struct _siq_plan_t *creat_f(struct _option *, char ***);

typedef struct _option {
	const char *name;		/* option name */
	creat_f *create;		/* create function */
	siq_exec_f *execute;		/* execute function */
	int flags;
} OPTION;


#define	c_data	p_un._c_data
#define	m_data	p_un._m_data
#define	o_data	p_un._o_data
#define	p_data	p_un._p_data
#define	re_data	p_un._re_data
#define	g_data	p_un._g_data
#define	u_data	p_un._u_data

siq_plan_t	*find_create(char ***);
char* 		strclone(char*);
siq_plan_t	*not_squish(siq_plan_t *);
siq_plan_t	*or_squish(siq_plan_t *);
siq_plan_t	*paren_squish(siq_plan_t *);

OPTION	*lookup_option(const char *);

creat_f	c_and;
creat_f	c_name;
creat_f	c_newer;
creat_f	c_regex;
creat_f	c_simple;
creat_f	c_size;
creat_f	c_type;
creat_f	c_user;
creat_f	c_userid;
creat_f	c_group;
creat_f	c_groupid;
creat_f	c_nouser;
creat_f	c_nogroup;

siq_exec_f	f_always_true;
siq_exec_f	f_closeparen;
siq_exec_f	f_expr;
siq_exec_f	f_name;
siq_exec_f	f_newer;
siq_exec_f	f_not;
siq_exec_f	f_openparen;
siq_exec_f	f_or;
siq_exec_f	f_path;
siq_exec_f	f_regex;
siq_exec_f	f_size;
siq_exec_f	f_type;
siq_exec_f	f_user;
siq_exec_f	f_group;
siq_exec_f	f_nogroup;
siq_exec_f	f_nouser;

extern int regexp_flags;
extern time_t pparser_now;
extern int custom_error;

#ifdef __cplusplus
}
#endif

#endif /* _SIQ_PARSER_PRIVATE_H_ */
