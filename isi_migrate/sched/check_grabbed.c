#include "check_needed.h"

SUITE_DEFINE_FOR_FILE(grabbed);

// This first tests are to test only the API for struct sched_grab in
// sched_grabbed.c

static void
create_grabbed_list(struct grabbed_list *g_list)
{
	grabbed_list_insert(g_list, "A", 1);
	grabbed_list_insert(g_list, "B", 2);
	grabbed_list_insert(g_list, "C", 3);
	grabbed_list_insert(g_list, "D", 4);
}

// Test find
TEST(grabbed_find, .attrs="overnight")
{
	struct grabbed_list g_list = {};
	struct grabbed_entry *g;

	create_grabbed_list(&g_list);
	g = grabbed_list_find(&g_list, "C");
	fail_unless(g != NULL);
	fail_unless(strcmp(g->dir_name, "C") == 0);
	grabbed_list_clear(&g_list);
}

// Test remove by entry ptr
TEST(grabbed_rm_by_ptr, .attrs="overnight")
{
	struct grabbed_list g_list = {};
	struct grabbed_entry *g;

	create_grabbed_list(&g_list);

	g = grabbed_list_find(&g_list, "C");
	fail_unless(g != NULL);
	grabbed_list_remove(&g_list, g);
	g = grabbed_list_find(&g_list, "C");
	fail_unless(g == NULL);
	grabbed_list_clear(&g_list);
}

// Test remove by dir_name
TEST(grabbed_rm_by_name, .attrs="overnight")
{
	struct grabbed_list g_list = {};
	struct grabbed_entry *g;

	create_grabbed_list(&g_list);

	// Test remove by dir_name for existing entry
	g = grabbed_list_find(&g_list, "B");
	fail_unless(g != NULL);
	fail_unless(strcmp(g->dir_name, "B") == 0);
	fail_unless(grabbed_list_remove_by_name(&g_list, "B") == true);
	fail_unless(grabbed_list_find(&g_list, "B") == NULL);
	
	// Test remove by dir_name for missing entry
	fail_unless(grabbed_list_remove_by_name(&g_list, "B") == false);
	grabbed_list_clear(&g_list);
}

TEST(grabbed_seen, .attrs="overnight")
{
	struct grabbed_list g_list = {};
	struct grabbed_entry *g;

	create_grabbed_list(&g_list);

	// Test seen initial setting as true
	SLIST_FOREACH(g, &g_list, next) {
		fail_unless(g->seen);
	}

	// First mark B & D unseen, A & C as seen
	g = grabbed_list_find(&g_list, "B");
	g->seen = true;
	g = grabbed_list_find(&g_list, "D");
	g->seen = true;

	// Test mark all unseen
	grabbed_list_mark_all_unseen(&g_list);
	SLIST_FOREACH(g, &g_list, next) {
		fail_unless(!g->seen);
	}

	// Test remove all unseen

	// First mark B & D seen, A & C as unseen
	g = grabbed_list_find(&g_list, "B");
	g->seen = true;
	g = grabbed_list_find(&g_list, "D");
	g->seen = true;

	grabbed_list_remove_all_unseen(&g_list);
	fail_unless(grabbed_list_find(&g_list, "B") != NULL);
	fail_unless(grabbed_list_find(&g_list, "D") != NULL);
	fail_unless(grabbed_list_find(&g_list, "A") == NULL);
	fail_unless(grabbed_list_find(&g_list, "C") == NULL);

	grabbed_list_clear(&g_list);
}
