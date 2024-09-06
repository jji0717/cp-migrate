/**
 * @file main.c
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
#include <errno.h>
#include <fcntl.h>
#include <fts.h>
#include <locale.h>
#include <regex.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "siq_parser_private.h"
#include "siq_ui_pred.h" 
#include "check_parser_utils.h"


int
main(int argc __unused, char *argv[] __unused)
{
	siq_ui_pred_t *pred = NULL;
	int res = 0;
	
	char predicate[] = "-size 5B";
	printf("parse  : %s\n", predicate);
	siq_plan_t *plan = siq_parser_parse(predicate, time(NULL));	
	printf("unparse: "); 
	printf(siq_parser_unparse(plan));
	printf("\n");

	struct stat st;
	st.st_size = 5;
	printf("%d", siq_parser_execute(plan, NULL, NULL, &st, time(NULL)));
	
	res = siq_ui_pred_from_plan(&pred, plan);
	if (res != 0) {
		printf("error: %d", res);
	}
	siq_ui_pred_print(pred, stdout);
	
	siq_plan_free(plan);

	return 0;
}
