#include <stdlib.h>
#include <sys/isi_assert.h>
#include <string.h>
#include <time.h>
#include <stdio.h>

#include "sched_grabbed.h"

/**
 * Insert a new entry into a grabbed list, settings its birth to the current
 * time, and setting seen to true.
 * Parameters:
 *   glist: The grabbed list to add to.
 *   dir_name: The grabbed directory name.
 *   ino: The inode of the grabbed directory name.
 *   
 * Notes: Will ASSERT if dir_name isn't in GRABBED state of PolicyID_NodeNum
 * Returns: Nothing
 */
void
grabbed_list_insert(struct grabbed_list *g_list, const char *dir_name,
    uint64_t ino)
{
	struct grabbed_entry *g_entry;

	g_entry = calloc(1, sizeof(struct grabbed_entry));
	ASSERT(g_entry);

	g_entry->dir_name = strdup(dir_name);
	g_entry->ino = ino;
	g_entry->timestamp = time(0);
	g_entry->seen = true;
	SLIST_INSERT_HEAD(g_list, g_entry, next);
}
	
/**
 * See if a matching directory name are found in a grabbed list
 * Parameters:
 *   glist: The grabbed list to search.
 *   dir_name: The grabbed directory name to search for.
 * Returns:
 *   If found in list, the pointer to the grabbed entry.
 *   If not found, NULL
 */
struct grabbed_entry *
grabbed_list_find(struct grabbed_list *g_list, const char *dir_name)
{
	struct grabbed_entry *g_entry;	

	SLIST_FOREACH(g_entry, g_list, next) {
		if (strcmp(dir_name, g_entry->dir_name) == 0)
			return g_entry;
	}
	return NULL;
}

/**
 * Mark every entry in the grabbed list as unseen.
 * Parameters:
 *   glist: The grabbed list.
 * Returns:
 *   Nothing
 */
void
grabbed_list_mark_all_unseen(struct grabbed_list *g_list)
{
	struct grabbed_entry *g_entry;	

	SLIST_FOREACH(g_entry, g_list, next) {
		g_entry->seen = false;
	}
}

static void
grabbed_entry_free(struct grabbed_entry *g_entry)
{
	free(g_entry->dir_name);
	free(g_entry);
}

/**
 * Remove any grabbed entry in the list that was marked as unseen.
 * Parameters:
 *   glist: The grabbed list.
 * Returns:
 *   Nothing
 */
void
grabbed_list_remove_all_unseen(struct grabbed_list *g_list)
{
	struct grabbed_entry *g_entry, *tmp;	

	SLIST_FOREACH_SAFE(g_entry, g_list, next, tmp) {
		if (!g_entry->seen) {
			SLIST_REMOVE(g_list, g_entry, grabbed_entry, next);
			grabbed_entry_free(g_entry);
		}
	}
}

/**
 * Remove a grabbed entry from the list specified by its pointer
 * Parameters:
 *   g_list: The grabbed list to search.
 *   g_entry: pointer to entry in the list.
 * Returns:
 *   Nothing
 */
void
grabbed_list_remove(struct grabbed_list *g_list, struct grabbed_entry *g_entry)
{
	SLIST_REMOVE(g_list, g_entry, grabbed_entry, next);
	grabbed_entry_free(g_entry);
}


/**
 * Remove a grabbed entry from the list by name
 * Parameters:
 *   g_list: The grabbed list to search.
 *   dir_name: a directory name in the GRABBED state
 * Returns:
 *   True: The directory name was in the list and deleted.
 *   False: The directory name was not in the list.
 */
bool
grabbed_list_remove_by_name(struct grabbed_list *g_list, const char *dir_name)
{
	struct grabbed_entry *g_entry;	

	g_entry = grabbed_list_find(g_list, dir_name);
	if (!g_entry)
		return false;

	SLIST_REMOVE(g_list, g_entry, grabbed_entry, next);
	grabbed_entry_free(g_entry);
	return true;
}

/**
 * Remove and free all allocated data in a grabbed list
 * g_list: The grabbed list to search.
 * Returns: Nothing
 */
void
grabbed_list_clear(struct grabbed_list *g_list)
{
	struct grabbed_entry *g_entry, *tmp;	

	SLIST_FOREACH_SAFE(g_entry, g_list, next, tmp) {
		SLIST_REMOVE(g_list, g_entry, grabbed_entry, next);
		grabbed_entry_free(g_entry);
	}
}
