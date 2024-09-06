#ifndef SELECTORS_H
#define SELECTORS_H


struct select {
	struct select *next;
	struct select *child;
	const char *path;
	int len;
	int select;
};


struct select * sel_build(const char *path, const char **selectors);
char * sel_export(struct select *s, int *len);
struct select * sel_import(char *buff);
void sel_free(struct select *s);
struct select * sel_find(struct select *s, char *path);
bool has_selected_node(struct select *s);

#endif
