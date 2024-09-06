#include <stdio.h>
#include <stdlib.h>
#include <isi_util/isi_error.h>

#include <check.h>
#include <isi_util/check_helper.h>

#include <isi_migrate/config/siq_job.h>
#include <isi_migrate/config/siq_util.h>

#include "coord.h"

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

SUITE_DEFINE_FOR_FILE(dwa, .suite_setup = suite_setup,
    .suite_teardown = suite_teardown, .test_setup = test_setup,
    .test_teardown = test_teardown, .timeout = 60,
    .mem_check = CK_MEM_DISABLE);

extern struct coord_ctx g_ctx;

static void
fake_ip_list(struct nodes *ns, int num)
{
	int i;

	ns->count = num;
	ns->ips = allocate_ip_list(num);

	for (i = 0; i < num; i++)
		sprintf(ns->ips[i], "12.34.56.%d", i);
}

static void
cleanup_nodes(struct nodes *ns)
{
	int i;
	
	if (ns == NULL)
		return;

	if (ns->ips)
		free_ip_list(ns->ips);

	if (ns->nodes) {
		for (i = 0; i < ns->count; i++) {
			if (ns->nodes[i]) {
				free(ns->nodes[i]->host);
				free(ns->nodes[i]);
			}
		}
		free(ns->nodes);
		ns->nodes = NULL;
	}
	free(ns);
}

static void
cleanup_workers(struct workers *ws)
{
	int i;

	if (ws == NULL)
		return;

	if (ws->workers) {
		for (i = 0; i < ws->count; i++)
			free(ws->workers[i]);
		free(ws->workers);
	}
	free(ws);
}

static void
conf_and_check(bool init)
{
	configure_workers(init);
	fail_unless(g_ctx.new_src_nodes == NULL);
	fail_unless(g_ctx.new_tgt_nodes == NULL);
	fail_if(g_ctx.workers == NULL);
	fail_if(g_ctx.src_nodes == NULL);
	fail_if(g_ctx.tgt_nodes == NULL);
}

TEST_FIXTURE(suite_setup)
{
	gc_job_summ = NULL;
}

TEST_FIXTURE(suite_teardown)
{
}

TEST_FIXTURE(test_setup)
{
	memset(&g_ctx, 0, sizeof(struct coord_ctx));
	g_ctx.rc.wperhost = 3;
	g_ctx.rc.max_wperpolicy = 40;
	g_ctx.timer = true;
}

TEST_FIXTURE(test_teardown)
{
	cleanup_nodes(g_ctx.new_src_nodes);
	cleanup_nodes(g_ctx.src_nodes);
	cleanup_nodes(g_ctx.new_tgt_nodes);
	cleanup_nodes(g_ctx.tgt_nodes);
	cleanup_workers(g_ctx.workers);
}

TEST(init_3s_3t, .attrs="overnight")
{
	g_ctx.new_src_nodes = calloc(1, sizeof(struct nodes));
	g_ctx.new_tgt_nodes = calloc(1, sizeof(struct nodes));
	fake_ip_list(g_ctx.new_src_nodes, 3);
	fake_ip_list(g_ctx.new_tgt_nodes, 3);

	conf_and_check(true);

	fail_unless(g_ctx.workers->count == 9);
}

TEST(init_5s_2t, .attrs="overnight")
{
	g_ctx.new_src_nodes = calloc(1, sizeof(struct nodes));
	g_ctx.new_tgt_nodes = calloc(1, sizeof(struct nodes));
	fake_ip_list(g_ctx.new_src_nodes, 5);
	fake_ip_list(g_ctx.new_tgt_nodes, 2);

	conf_and_check(true);

	fail_unless(g_ctx.workers->count == 6);
}

TEST(init_5s_5t_gc_lose_2s, .attrs="overnight")
{
	int i; 
	g_ctx.new_src_nodes = calloc(1, sizeof(struct nodes));
	g_ctx.new_tgt_nodes = calloc(1, sizeof(struct nodes));
	fake_ip_list(g_ctx.new_src_nodes, 5);
	fake_ip_list(g_ctx.new_tgt_nodes, 5);
	
	conf_and_check(true);

	fail_unless(g_ctx.workers->count == 15);

	g_ctx.workers->connected = 15;
	for (i = 0; i < 15; i++)
		g_ctx.workers->workers[i]->socket = 1;

	g_ctx.new_src_nodes = calloc(1, sizeof(struct nodes));
	fake_ip_list(g_ctx.new_src_nodes, 3);

	conf_and_check(false);

	fail_unless(g_ctx.workers->count == 9);
	fail_unless(g_ctx.workers->connected == 9);
}

TEST(init_5s_3t_gc_gain_3t, .attrs="overnight")
{
	int i; 
	g_ctx.new_src_nodes = calloc(1, sizeof(struct nodes));
	g_ctx.new_tgt_nodes = calloc(1, sizeof(struct nodes));
	fake_ip_list(g_ctx.new_src_nodes, 5);
	fake_ip_list(g_ctx.new_tgt_nodes, 3);
	
	conf_and_check(true);

	fail_unless(g_ctx.workers->count == 9);

	g_ctx.workers->connected = 9;
	for (i = 0; i < 9; i++)
		g_ctx.workers->workers[i]->socket = 1;

	g_ctx.new_tgt_nodes = calloc(1, sizeof(struct nodes));
	fake_ip_list(g_ctx.new_tgt_nodes, 6);

	conf_and_check(false);

	fail_unless(g_ctx.workers->count == 15);
	fail_unless(g_ctx.workers->connected == 9);	
}

TEST(init_3s_3t_gc_gain_2s_gc_gain_2t, .attrs="overnight")
{
	int i; 
	g_ctx.new_src_nodes = calloc(1, sizeof(struct nodes));
	g_ctx.new_tgt_nodes = calloc(1, sizeof(struct nodes));
	fake_ip_list(g_ctx.new_src_nodes, 3);
	fake_ip_list(g_ctx.new_tgt_nodes, 3);
	
	conf_and_check(true);

	fail_unless(g_ctx.workers->count == 9);

	g_ctx.workers->connected = 9;
	for (i = 0; i < 9; i++)
		g_ctx.workers->workers[i]->socket = 1;

	g_ctx.new_src_nodes = calloc(1, sizeof(struct nodes));
	fake_ip_list(g_ctx.new_src_nodes, 5);

	conf_and_check(false);

	fail_unless(g_ctx.workers->count == 9);
	fail_unless(g_ctx.workers->connected == 9);	

	g_ctx.new_tgt_nodes = calloc(1, sizeof(struct nodes));
	fake_ip_list(g_ctx.new_tgt_nodes, 5);

	conf_and_check(false);

	fail_unless(g_ctx.workers->count == 15);
	fail_unless(g_ctx.workers->connected == 9);	
}

TEST(compare_matching_ip_lists, .attrs="overnight")
{
	char *ips_a[] = {"1.2.3.4", "1.2.3.5", "1.2.3.6", "1.2.3.7", "1.2.3.8"};
	char *ips_b[] = {"1.2.3.8", "1.2.3.7", "1.2.3.6", "1.2.3.5", "1.2.3.4"};

	fail_unless(ip_lists_match(5, ips_a, 5, ips_b));
}

TEST(compare_mismatching_ip_lists, .attrs="overnight")
{
	char *ips_a[] = {"1.2.3.4", "1.2.3.5", "1.2.3.6", "1.2.3.7", "1.2.3.8"};
	char *ips_b[] = {"1.9.3.8", "1.2.3.7", "1.2.3.6", "1.2.3.5", "1.2.3.4"};

	fail_if(ip_lists_match(5, ips_a, 5, ips_b));
}

TEST(compare_matching_ip_lists_IPv6, .attrs="overnight")
{
	char *ips_a[] = {"1:2:3::4", "1:2:3::5", "1:2:3::6", "1:2:3::7", "1:2:3::8"};
	char *ips_b[] = {"1:2:3::8", "1:2:3::7", "1:2:3::6", "1:2:3::5", "1:2:3::4"};

	fail_unless(ip_lists_match(5, ips_a, 5, ips_b));
}

TEST(compare_mismatching_ip_lists_IPv6, .attrs="overnight")
{
	char *ips_a[] = {"1:2:3::4", "1:2:3::5", "1:2:3::6", "1:2:3::7", "1:2:3::8"};
	char *ips_b[] = {"1:9:3::8", "1:2:3::7", "1:2:3::6", "1:2:3::5", "1:2:3::4"};

	fail_if(ip_lists_match(5, ips_a, 5, ips_b));
}
