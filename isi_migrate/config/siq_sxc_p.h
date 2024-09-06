/**
 * @file siq_sxc_p.h
 * TSM Job Statistics/Progress private API
 * (c) 2006 Isilon Systems
 */
#ifndef SIQ_SXC_P_H_
#define SIQ_SXC_P_H_

/* Require correct parent */
#ifndef SIQ_SXC_C
#error "This file may ionly be included by siq_sxc.c"
#endif /* SIQ_SXC_C */

#include <stddef.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#include <libxml/tree.h>
#include <libxml/parser.h>
#include <libxml/encoding.h>

#include "siq_sxc.h"

#ifndef MAXPATHLEN
#define MAXPATHLEN 1024
#endif /* MAXPATHLEN */

#ifdef SXC_DEBUG
#define dprintf(fmt, args...) printf(fmt "\n", ##args)
#else
#define dprintf(fmt, args...)
#endif /* SXC_DEBUG */

/*
 * Our XML settings
 */
const xmlChar	*SXC_XMLVER  = "1.0";
const xmlNsPtr	SXC_XMLNS =(xmlNsPtr) 0;
const int	SXC_ENC = XML_CHAR_ENCODING_UTF8;
const char	*SXC_ENCNAME = "UTF-8";
const int 	SXC_POPT = XML_PARSE_NONET;
const char	*SXC_TMPNAME = ".tmp";

const char *sxc_cvt_name[CVT_TYPES + 1] = {
	"short", "int", "long", "long long", "float", "double", "array", 
	"string", "bool", "dict", "map", "list", "complex", "ignored", "N/A"
};

/*
 * Handy maps/dictionaries
 */
char *sxc_bool_dict[] = {
	"false", "true", 
	NULL
};


/**
 ** Handlers and dispatch tables
 **/

/*
 * X2B
 */
typedef int (*sxc_x2b_handler_t)(xmlNodePtr, sxc_cvt_t *, char *);

#define DEFINE_SX2B_HANDLER(fmt,type)					\
static int								\
sxc_x2b_##fmt(xmlNodePtr node, sxc_cvt_t *conv, char *bp)		\
{									\
	char *text = (char *)xmlNodeGetContent(node);			\
	int err;							\
	dprintf("\tsxc_x2b_%s --> %s", #fmt, text);			\
	if (NULL==text) {						\
		return EINVAL;						\
	}								\
	err = sscanf(text, "%"#fmt, (type *)bp) ? 0 : EINVAL;		\
	xmlFree(text);							\
	return err;							\
}

#define DECLARE_X2B_HANDLER(type)					\
static int								\
sxc_x2b_##type(xmlNodePtr, sxc_cvt_t *, char *)

DECLARE_X2B_HANDLER(u);
DECLARE_X2B_HANDLER(d);
DECLARE_X2B_HANDLER(ld);
DECLARE_X2B_HANDLER(lld);
DECLARE_X2B_HANDLER(f);
DECLARE_X2B_HANDLER(lf);
DECLARE_X2B_HANDLER(arr);
DECLARE_X2B_HANDLER(str);
DECLARE_X2B_HANDLER(bool);
DECLARE_X2B_HANDLER(dict);
DECLARE_X2B_HANDLER(map);
DECLARE_X2B_HANDLER(list);
DECLARE_X2B_HANDLER(cmplx);

static sxc_x2b_handler_t 
sxc_x2b_dispatch[CVT_TYPES + 1] = {
	sxc_x2b_u,
	sxc_x2b_d,
	sxc_x2b_ld,
	sxc_x2b_lld,
	sxc_x2b_f,
	sxc_x2b_lf,
	sxc_x2b_arr,
	sxc_x2b_str,
	sxc_x2b_bool,
	sxc_x2b_dict,
	sxc_x2b_map,
	sxc_x2b_list,
	sxc_x2b_cmplx,
	NULL, NULL
};

/*
 * B2X
 */
typedef int (*sxc_b2x_handler_t)(char *, sxc_cvt_t *, xmlNodePtr);

#define DEFINE_SB2X_HANDLER(fmt,type)					\
static int								\
sxc_b2x_##fmt(char *bp, sxc_cvt_t *conv, xmlNodePtr node)		\
{									\
	char text[80];							\
	int rc;								\
	if (!(rc=snprintf(text,sizeof(text),"%"#fmt,*(type *)bp)) ||	\
	    rc == sizeof(text)) {					\
		return EINVAL;						\
	}								\
	dprintf("\tsxc_b2x_%s --> %s", #fmt, text);			\
	xmlNodeSetContent(node, BAD_CAST text);				\
	return 0;							\
}

#define DECLARE_B2X_HANDLER(type)					\
static int								\
sxc_b2x_##type(char *, sxc_cvt_t *, xmlNodePtr)

DECLARE_B2X_HANDLER(u);
DECLARE_B2X_HANDLER(d);
DECLARE_B2X_HANDLER(ld);
DECLARE_B2X_HANDLER(lld);
DECLARE_B2X_HANDLER(f);
DECLARE_B2X_HANDLER(lf);
DECLARE_B2X_HANDLER(arr);
DECLARE_B2X_HANDLER(str);
DECLARE_B2X_HANDLER(bool);
DECLARE_B2X_HANDLER(dict);
DECLARE_B2X_HANDLER(map);
DECLARE_B2X_HANDLER(list);
DECLARE_B2X_HANDLER(cmplx);

static sxc_b2x_handler_t 
sxc_b2x_dispatch[CVT_TYPES + 1] = {
	sxc_b2x_u,
	sxc_b2x_d,
	sxc_b2x_ld,
	sxc_b2x_lld,
	sxc_b2x_f,
	sxc_b2x_lf,
	sxc_b2x_arr,
	sxc_b2x_str,
	sxc_b2x_bool,
	sxc_b2x_dict,
	sxc_b2x_map,
	sxc_b2x_list,
	sxc_b2x_cmplx,
	NULL, NULL
};

#endif /*SIQ_SXC_P_H_*/
