#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "isirep.h"
#include "selectors.h"


static void
sel_print(struct select *s, int indent)
{
	char fmt[256];

	while (s) {
		sprintf(fmt, "%%%ds %%d %%.%ds (%%p)", indent, s->len);
		log(DEBUG, fmt, "", s->select, s->path, s);
		if (s->child)
			sel_print(s->child, indent + 2);
		s = s->next;
	}
}

/*
 * Function to check if the current select tree has any node
 * which has select = 1
 */
bool
has_selected_node(struct select *s)
{
	while (s) {
		if (s->select)
			return true;
		if (has_selected_node(s->child))
			return true;
		s = s->next;
	}
	return false;
}

/* We know the list is in alpha order; some of this code depends on it. */
static struct select *
_sel_build(struct select *p, const char **sels, int *i, int start)
{
	struct select *s = 0, *res = 0, *prev = 0;
	const char *c, *path;

	for (; sels[*i]; (*i)++) {
		path = sels[*i] + start + (strchr("+-", *sels[*i]) ? 1 : 0);
		if (strncmp(path - p->len - 1, p->path, p->len) ||
		    path[-1] != '/')
			break;
		if ((!prev || strncmp(path, prev->path, prev->len) ||
		    path[prev->len] != '/')) {
			s = calloc(1, sizeof *s);
			res = res ? res : s;
			s->path = path;
			s->len = strlen(s->path);
			s->select = strchr(path, '/') ? p->select :
			    *sels[*i] != '-';
		}
		if ((c = strchr(path, '/'))) {
			s->len = c - path;
			s->child = _sel_build(s, sels, i, start + s->len + 1);
		}
		if (prev && prev != s)
			prev->next = s;
		prev = s;
	}

	(*i)--;
	return res;
}

struct select *
sel_build(const char *path, const char **selectors)
{
	struct select *s;
	int i = 0;

	ASSERT(path);
	ASSERT(selectors);

	s = calloc(1, sizeof *s);
	s->path = path;
	s->len = strlen(path);
	/* We default to not transferring the base path.  This is because when
	 * users specify only a root path and no includes, the coord's conf.c
	 * actually adds in a single include which is the root_path, thus 
	 * causing the strcmp to evaluate to true. */
	s->select = 0;
	if (!strcmp(path, strchr(selectors[0], '/'))) {
		s->select = *selectors[0] != '-';
		i++;
	}
	s->child = _sel_build(s, selectors, &i, s->len + 1);
	sel_print(s, 0);
	return s;
}

void
sel_free(struct select *s)
{
	struct select *s_old;
	while (s != NULL) {
		sel_free(s->child);
		s_old = s;
		s = s_old->next;
		free(s_old);
	}
}

struct select *
sel_find(struct select *s, char *path)
{
	for (s = s ? s->child : 0; s; s = s->next)
		if (!strncmp(s->path, path, s->len) && strlen(path) == s->len)
			return s;
	return 0;
}

static void
sel_count(struct select *s, int *n, int *c)
{
	for (; s; s = s->next) {
		(*n)++;
		*c += s->len + 1;
		if (s->child)
			sel_count(s->child, n, c);
	}
}


static void
_sel_export(struct select *s, struct select **arr, char **str, intptr_t fixup,
    int *i)
{
	struct select *tmp, *child;

	for (; s; s = s->next) {
		**arr = *s;
		tmp = *arr;
		(*arr)++;

		strncpy(*str, tmp->path, tmp->len);
		(*str)[tmp->len] = 0;
		tmp->path = *str - fixup;
		*str += tmp->len + 1;

		if (tmp->child) {
			child = (struct select *)((char*)*arr - fixup);
			_sel_export(tmp->child, arr, str, fixup, i);
			tmp->child = child;
		}
		if (tmp->next)
			tmp->next = (struct select *)((char *)*arr - fixup);
	}
}

char *
sel_export(struct select *s, int *len)
{
	struct select *arr, *next;
	int n = 0, c = 0;
	char *res, *buf, *str;

	ASSERT(s);
	ASSERT(len);

	next = s->next; /* Cut off siblings of root selector. */
	s->next = 0;
	sel_count(s, &n, &c);
	ASSERT(n >= 0);
	*len = n * sizeof *s + c + sizeof n * 2;
	res = buf = malloc(*len);

	*(int *)buf = *len;
	buf += sizeof n;
	*(int *)buf = n;
	buf += sizeof n;

	arr = (struct select *)buf;
	str = buf + n * sizeof *s;
	_sel_export(s, &arr, &str, (intptr_t)buf, len);
	s->next = next;

	return res;
}

struct select *
sel_import(char *_buf)
{
	struct select *s, *res = NULL;
	int len, n, i;
	char *buf, *c;

	ASSERT(_buf);

	len = *(int *)_buf;

	if (!len)
		goto out;

	_buf += sizeof i;
	n = *(int *)_buf;
	ASSERT(n >= 0);
	_buf += sizeof i;

	buf = malloc(len);
	memcpy(buf, _buf, len);
	res = (struct select *)buf;
	c = buf + n * sizeof *s;
	for (i = 0, s = (struct select *)buf; i < n; i++, s++) {
		if (s->next) {
			ASSERT((uintptr_t)s->next < (uintptr_t)(unsigned)len);
			s->next = (struct select *)((char *)s->next +
			    (intptr_t)res);
		}
		if (s->child) {
			ASSERT((uintptr_t)s->child < (uintptr_t)(unsigned)len);
			s->child = (struct select *)((char *)s->child +
			    (intptr_t)res);
		}
		s->path = c;
		c += s->len + 1;
	}
	sel_print(res, 0);
out:
	return res;
}

