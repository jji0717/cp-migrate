#include <stdio.h>
#include <stdlib.h>
#include <isi_util/isi_error.h>

#include <check.h>
#include <isi_util/check_helper.h>
#include <isi_migrate/migr/siq_workers_utils.c>

#include "coord.h"


TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);

SUITE_DEFINE_FOR_FILE(migr_stat, .suite_setup = suite_setup,
    .suite_teardown = suite_teardown, .timeout = 60);
//    .mem_check = CK_MEM_DISABLE);


TEST_FIXTURE(suite_setup)
{
}

TEST_FIXTURE(suite_teardown)
{
}

static void
randomize_stat_dirs_src(struct siq_stat_dirs_src *src)
{
	src->visited = rand();
}

static void
randomize_stat_dirs_dst(struct siq_stat_dirs_dst *dst)
{
	dst->deleted = rand();
}

static void
randomize_stat_srcdst(struct siq_stat_srcdst *srcdst)
{
	srcdst->src = rand();
	srcdst->dst = rand();
}

static void
randomize_stat_dirs(struct siq_stat_dirs *dirs)
{
	randomize_stat_dirs_src(dirs->src);
	randomize_stat_dirs_dst(dirs->dst);
	randomize_stat_srcdst(dirs->created);
	randomize_stat_srcdst(dirs->deleted);
	randomize_stat_srcdst(dirs->linked);
	randomize_stat_srcdst(dirs->unlinked);
	dirs->replicated = rand();
}

static void
randomize_stat_files_replicated(struct siq_stat_files_replicated *replicated)
{
	replicated->selected = rand();
	replicated->transferred = rand();
	replicated->new_files = rand();
	replicated->updated_files = rand();
	replicated->regular = rand();
	replicated->files_with_ads = rand();
	replicated->ads_streams = rand();
	replicated->symlinks = rand();
	replicated->block_specs = rand();
	replicated->char_specs = rand();
	replicated->sockets = rand();
	replicated->fifos = rand();
	replicated->hard_links = rand();
}

static void
randomize_stat_files_skipped(struct siq_stat_files_skipped *skipped)
{
	skipped->up_to_date = rand();
	skipped->user_conflict = rand();
	skipped->error_io = rand();
	skipped->error_net = rand();
	skipped->error_checksum = rand();
}

static void
randomize_stat_files(struct siq_stat_files *files)
{
	files->total = rand();
	randomize_stat_files_replicated(files->replicated);
	randomize_stat_srcdst(files->deleted);
	randomize_stat_srcdst(files->linked);
	randomize_stat_srcdst(files->unlinked);
	randomize_stat_files_skipped(files->skipped);
}

static void
randomize_stat_bytes_data(struct siq_stat_bytes_data *data)
{
	data->total = rand();
	data->file = rand();
	data->sparse = rand();
	data->unchanged = rand();
}

static void
randomize_stat_bytes_network(struct siq_stat_bytes_network *network)
{
	network->total = rand();
	network->to_target = rand();
	network->to_source = rand();
}

static void
randomize_stat_bytes(struct siq_stat_bytes *bytes)
{
	bytes->transferred = rand();
	bytes->recoverable = rand();
	randomize_stat_srcdst(bytes->recovered);
	randomize_stat_bytes_data(bytes->data);
	randomize_stat_bytes_network(bytes->network);
}

static void
randomize_stat_change_compute(struct siq_stat_change_compute *change_compute)
{
	change_compute->lins_total = rand();
	change_compute->dirs_new = rand();
	change_compute->dirs_deleted = rand();
	change_compute->dirs_moved = rand();
	change_compute->dirs_changed = rand();
	change_compute->files_new = rand();
	change_compute->files_linked = rand();
	change_compute->files_unlinked = rand();
	change_compute->files_changed = rand();
}

static void
randomize_stat_change_transfer(struct siq_stat_change_transfer *change_transfer)
{
	change_transfer->hash_exceptions_found = rand();
	change_transfer->hash_exceptions_fixed = rand();
	change_transfer->flipped_lins = rand();
	change_transfer->corrected_lins = rand();
	change_transfer->resynced_lins = rand();
}

static void
randomize_stat_compliance(struct siq_stat_compliance *compliance)
{
	compliance->resync_compliance_dirs_new = rand();
	compliance->resync_compliance_dirs_linked = rand();
	compliance->resync_conflict_files_new = rand();
	compliance->resync_conflict_files_linked = rand();
	compliance->committed_files = rand();
}

static void
randomize_stats(struct siq_stat *stat)
{
	randomize_stat_dirs(stat->dirs);
	randomize_stat_files(stat->files);
	randomize_stat_bytes(stat->bytes);
	randomize_stat_change_compute(stat->change_compute);
	randomize_stat_change_transfer(stat->change_transfer);
	randomize_stat_compliance(stat->compliance);
}

static bool
compare_pworker_tw_stats(struct siq_stat *stats,
    struct pworker_tw_stat_msg *ret, uint64_t dirlin, uint64_t filelin,
    uint32_t ver)
{
	bool same;

	same = (stats->dirs->src->visited == ret->dirs_visited) &&
	    (stats->dirs->dst->deleted == ret->dirs_dst_deleted) &&
	    (stats->files->total == ret->files_total) &&
	    (stats->files->replicated->selected == ret->files_replicated_selected) &&
	    (stats->files->replicated->transferred == ret->files_replicated_transferred) &&
	    (stats->files->replicated->new_files == ret->files_replicated_new_files) &&
	    (stats->files->replicated->updated_files == ret->files_replicated_updated_files) &&
	    (stats->files->replicated->files_with_ads == ret->files_replicated_with_ads) &&
	    (stats->files->replicated->ads_streams == ret->files_replicated_ads_streams) &&
	    (stats->files->replicated->symlinks == ret->files_replicated_symlinks) &&
	    (stats->files->replicated->block_specs == ret->files_replicated_block_specs) &&
	    (stats->files->replicated->char_specs == ret->files_replicated_char_specs) &&
	    (stats->files->replicated->sockets == ret->files_replicated_sockets) &&
	    (stats->files->replicated->fifos == ret->files_replicated_fifos) &&
	    (stats->files->replicated->hard_links == ret->files_replicated_hard_links) &&
	    (stats->files->deleted->src == ret->files_deleted_src) &&
	    (stats->files->deleted->dst == ret->files_deleted_dst) &&
	    (stats->files->skipped->up_to_date == ret->files_skipped_up_to_date) &&
	    (stats->files->skipped->user_conflict == ret->files_skipped_user_conflict) &&
	    (stats->files->skipped->error_io == ret->files_skipped_error_io) &&
	    (stats->files->skipped->error_net == ret->files_skipped_error_net) &&
	    (stats->files->skipped->error_checksum == ret->files_skipped_error_checksum) &&
	    (stats->bytes->transferred == ret->bytes_transferred) &&
	    (stats->bytes->data->total == ret->bytes_data_total) &&
	    (stats->bytes->data->file == ret->bytes_data_file) &&
	    (stats->bytes->data->sparse == ret->bytes_data_sparse) &&
	    (stats->bytes->data->unchanged == ret->bytes_data_unchanged) &&
	    (stats->bytes->network->total == ret->bytes_network_total) &&
	    (stats->bytes->network->to_target == ret->bytes_network_to_target) &&
	    (stats->bytes->network->to_source == ret->bytes_network_to_source) &&
	    (dirlin == ret->twlin) &&
	    (filelin == ret->filelin);

	return same;
}

static bool
compare_sworker_tw_stats(struct siq_stat *stats,
    struct sworker_tw_stat_msg *ret, uint32_t ver)
{
	bool same;

	same = (stats->dirs->src->visited == ret->dirs_visited) &&
	    (stats->dirs->dst->deleted == ret->dirs_dst_deleted) &&
	    (stats->files->total == ret->files_total) &&
	    (stats->files->replicated->selected == ret->files_replicated_selected) &&
	    (stats->files->replicated->transferred == ret->files_replicated_transferred) &&
	    (stats->files->deleted->src == ret->files_deleted_src) &&
	    (stats->files->deleted->dst == ret->files_deleted_dst) &&
	    (stats->files->skipped->up_to_date == ret->files_skipped_up_to_date) &&
	    (stats->files->skipped->user_conflict == ret->files_skipped_user_conflict) &&
	    (stats->files->skipped->error_io == ret->files_skipped_error_io) &&
	    (stats->files->skipped->error_net == ret->files_skipped_error_net) &&
	    (stats->files->skipped->error_checksum == ret->files_skipped_error_checksum);
	if (ver >= MSG_VERSION_HALFPIPE) {
		same &= (stats->compliance->committed_files == ret->committed_files);
	}

	return same;
}

static bool
compare_pworker_stf_stats(struct siq_stat *stats,
    struct pworker_stf_stat_msg *ret, uint32_t ver)
{
	bool same;

	same = (stats->dirs->created->src == ret->dirs_created_src) &&
	    (stats->dirs->deleted->src == ret->dirs_deleted_src) &&
	    (stats->dirs->linked->src == ret->dirs_linked_src) &&
	    (stats->dirs->unlinked->src == ret->dirs_unlinked_src) &&
	    (stats->dirs->replicated == ret->dirs_replicated) &&
	    (stats->files->total == ret->files_total) &&
	    (stats->files->replicated->new_files == ret->files_replicated_new_files) &&
	    (stats->files->replicated->updated_files == ret->files_replicated_updated_files) &&
	    (stats->files->replicated->regular == ret->files_replicated_regular) &&
	    (stats->files->replicated->files_with_ads == ret->files_replicated_with_ads) &&
	    (stats->files->replicated->ads_streams == ret->files_replicated_ads_streams) &&
	    (stats->files->replicated->symlinks == ret->files_replicated_symlinks) &&
	    (stats->files->replicated->block_specs == ret->files_replicated_block_specs) &&
	    (stats->files->replicated->char_specs == ret->files_replicated_char_specs) &&
	    (stats->files->replicated->sockets == ret->files_replicated_sockets) &&
	    (stats->files->replicated->fifos == ret->files_replicated_fifos) &&
	    (stats->files->replicated->hard_links == ret->files_replicated_hard_links) &&
	    (stats->files->deleted->src == ret->files_deleted_src) &&
	    (stats->files->linked->src == ret->files_linked_src) &&
	    (stats->files->unlinked->src == ret->files_unlinked_src) &&
	    (stats->bytes->data->total == ret->bytes_data_total) &&
	    (stats->bytes->data->file == ret->bytes_data_file) &&
	    (stats->bytes->data->file == ret->bytes_data_file) &&
	    (stats->bytes->data->sparse == ret->bytes_data_sparse) &&
	    (stats->bytes->network->total == ret->bytes_network_total) &&
	    (stats->bytes->network->to_target == ret->bytes_network_to_target) &&
	    (stats->bytes->network->to_source == ret->bytes_network_to_source) &&
	    (stats->change_compute->lins_total == ret->cc_lins_total) &&
	    (stats->change_compute->dirs_new == ret->cc_dirs_new) &&
	    (stats->change_compute->dirs_deleted == ret->cc_dirs_deleted) &&
	    (stats->change_compute->dirs_moved == ret->cc_dirs_moved) &&
	    (stats->change_compute->dirs_changed == ret->cc_dirs_changed) &&
	    (stats->change_compute->files_new == ret->cc_files_new) &&
	    (stats->change_compute->files_linked == ret->cc_files_linked) &&
	    (stats->change_compute->files_unlinked == ret->cc_files_unlinked) &&
	    (stats->change_compute->files_changed == ret->cc_files_changed) &&
	    (stats->change_transfer->hash_exceptions_found == ret->ct_hash_exceptions_found) &&
	    (stats->change_transfer->hash_exceptions_fixed == ret->ct_hash_exceptions_fixed) &&
	    (stats->change_transfer->flipped_lins == ret->ct_flipped_lins) &&
	    (stats->change_transfer->corrected_lins == ret->ct_corrected_lins) &&
	    (stats->change_transfer->resynced_lins == ret->ct_resynced_lins);

	if (ver >= MSG_VERSION_HALFPIPE) {
		same &= (stats->compliance->resync_compliance_dirs_new == ret->resync_compliance_dirs_new) &&
		    (stats->compliance->resync_compliance_dirs_linked == ret->resync_compliance_dirs_linked) &&
		    (stats->compliance->resync_conflict_files_new == ret->resync_conflict_files_new) &&
		    (stats->compliance->resync_conflict_files_linked == ret->resync_conflict_files_linked);
	}

	return same;
}

static bool
compare_sworker_stf_stats(struct siq_stat *stats,
    struct sworker_stf_stat_msg *ret, uint32_t ver)
{
	bool same;

	same = (stats->dirs->created->dst == ret->dirs_created_dst) &&
	    (stats->dirs->deleted->dst == ret->dirs_deleted_dst) &&
	    (stats->dirs->linked->dst == ret->dirs_linked_dst) &&
	    (stats->dirs->unlinked->dst == ret->dirs_unlinked_dst) &&
	    (stats->files->deleted->dst == ret->files_deleted_dst) &&
	    (stats->files->linked->dst == ret->files_linked_dst) &&
	    (stats->files->unlinked->dst == ret->files_unlinked_dst);
	if (ver >= MSG_VERSION_HALFPIPE) {
		same &= (stats->compliance->committed_files == ret->committed_files);
	}

	return same;	
}

static void
pworker_tw_stat_test(uint32_t ver)
{
	uint64_t dirlin = 5;
	uint64_t filelin = 10;
	unsigned type;
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct pworker_tw_stat_msg ret = {};
	struct siq_stat *cur_stats;
	struct siq_stat *last_stats;
	struct isi_error *error = NULL;

	cur_stats = siq_stat_create();
	last_stats = siq_stat_create();
	randomize_stats(cur_stats);

	pworker_tw_stat_msg_pack(&msg, cur_stats, last_stats, dirlin, filelin,
	    ver);

	type = ver >= MSG_VERSION_HALFPIPE ?
	    build_ript_msg_type(PWORKER_TW_STAT_MSG) : OLD_PWORKER_TW_STAT_MSG;
	fail_unless(type == msg.head.type);
	pworker_tw_stat_msg_unpack(&msg, ver, &ret, &error);
	fail_unless(error == NULL);

	compare_pworker_tw_stats(cur_stats, &ret, dirlin, filelin, ver);

	siq_stat_free(cur_stats);
	siq_stat_free(last_stats);
}

static void
sworker_tw_stat_test(uint32_t ver)
{
	unsigned type;
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct sworker_tw_stat_msg ret = {};
	struct siq_stat *cur_stats;
	struct siq_stat *last_stats;
	struct isi_error *error = NULL;

	cur_stats = siq_stat_create();
	last_stats = siq_stat_create();
	randomize_stats(cur_stats);

	sworker_tw_stat_msg_pack(&msg, cur_stats, last_stats, ver);

	type = ver >= MSG_VERSION_HALFPIPE ?
	    build_ript_msg_type(SWORKER_TW_STAT_MSG) : OLD_SWORKER_TW_STAT_MSG;
	fail_unless(type == msg.head.type);
	sworker_tw_stat_msg_unpack(&msg, ver, &ret, &error);
	fail_unless(error == NULL);

	compare_sworker_tw_stats(cur_stats, &ret, ver);

	siq_stat_free(cur_stats);
	siq_stat_free(last_stats);
}

static void
pworker_stf_stat_test(uint32_t ver)
{
	unsigned type;
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct pworker_stf_stat_msg ret = {};
	struct siq_stat *cur_stats;
	struct siq_stat *last_stats;
	struct isi_error *error = NULL;

	cur_stats = siq_stat_create();
	last_stats = siq_stat_create();
	randomize_stats(cur_stats);

	pworker_stf_stat_msg_pack(&msg, cur_stats, last_stats, ver);

	type = ver >= MSG_VERSION_HALFPIPE ?
	    build_ript_msg_type(PWORKER_STF_STAT_MSG) :
	    OLD_PWORKER_STF_STAT_MSG;
	fail_unless(type == msg.head.type);
	pworker_stf_stat_msg_unpack(&msg, ver, &ret, &error);
	fail_unless(error == NULL);

	compare_pworker_stf_stats(cur_stats, &ret, ver);

	siq_stat_free(cur_stats);
	siq_stat_free(last_stats);
}

static void
sworker_stf_stat_test(uint32_t ver)
{
	unsigned type;
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct sworker_stf_stat_msg ret = {};
	struct siq_stat *cur_stats;
	struct siq_stat *last_stats;
	struct isi_error *error = NULL;

	cur_stats = siq_stat_create();
	last_stats = siq_stat_create();
	randomize_stats(cur_stats);

	sworker_stf_stat_msg_pack(&msg, cur_stats, last_stats, ver);

	type = ver >= MSG_VERSION_HALFPIPE ?
	    build_ript_msg_type(SWORKER_STF_STAT_MSG) :
	    OLD_SWORKER_STF_STAT_MSG;
	fail_unless(type == msg.head.type);
	sworker_stf_stat_msg_unpack(&msg, ver, &ret, &error);
	fail_unless(error == NULL);

	compare_sworker_stf_stats(cur_stats, &ret, ver);

	siq_stat_free(cur_stats);
	siq_stat_free(last_stats);
}


TEST(test_pworker_tw_stat_ript_msg, .attrs="overnight")
{
	uint32_t ver = MSG_VERSION_RIPTIDE;

	pworker_tw_stat_test(ver);
}


TEST(test_pworker_tw_stat_hp_msg, .attrs="overnight")
{
	uint32_t ver = MSG_VERSION_HALFPIPE;

	pworker_tw_stat_test(ver);
}

TEST(test_sworker_tw_stat_ript_msg, .attrs="overnight")
{
	uint32_t ver = MSG_VERSION_RIPTIDE;

	sworker_tw_stat_test(ver);
}

TEST(test_sworker_tw_stat_hp_msg, .attrs="overnight")
{
	uint32_t ver = MSG_VERSION_HALFPIPE;

	sworker_tw_stat_test(ver);
}

TEST(test_pworker_stf_stat_ript_msg, .attrs="overnight")
{
	uint32_t ver = MSG_VERSION_RIPTIDE;

	pworker_stf_stat_test(ver);
}


TEST(test_pworker_stf_stat_hp_msg, .attrs="overnight")
{
	uint32_t ver = MSG_VERSION_HALFPIPE;

	pworker_stf_stat_test(ver);
}

TEST(test_sworker_stf_stat_ript_msg, .attrs="overnight")
{
	uint32_t ver = MSG_VERSION_RIPTIDE;

	sworker_stf_stat_test(ver);
}


TEST(test_sworker_stf_stat_hp_msg, .attrs="overnight")
{
	uint32_t ver = MSG_VERSION_HALFPIPE;

	sworker_stf_stat_test(ver);
}
