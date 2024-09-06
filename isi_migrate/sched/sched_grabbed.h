#ifndef __SCHED_GRABBED_H__
#define __SCHED_GRABBED_H__

#include <sys/types.h>
#include <sys/queue.h>
#include <stdbool.h>

struct grabbed_entry {
	char *dir_name;
	uint64_t ino;
	time_t timestamp;
	bool seen;
	SLIST_ENTRY(grabbed_entry) next;
};
SLIST_HEAD(grabbed_list, grabbed_entry);

void
grabbed_list_insert(struct grabbed_list *g_list, const char *dir_name,
    uint64_t ino);

struct grabbed_entry *
grabbed_list_find(struct grabbed_list *g_list, const char *dir_name);

void grabbed_list_mark_all_unseen(struct grabbed_list *g_list);

void grabbed_list_remove_all_unseen(struct grabbed_list *g_list);

void grabbed_list_remove(struct grabbed_list *g_list,
    struct grabbed_entry *g_entry);

bool
grabbed_list_remove_by_name(struct grabbed_list *g_list, const char *dir_name);

void
grabbed_list_clear(struct grabbed_list *g_list);

#endif // __SCHED_GRABBED_H__
