#include "isirep.h"
#include "isi_migrate/config/siq_job.h"
#include <ctype.h>
#include "siq_workers_utils.h"
#include <isi_ufp/isi_ufp.h>
#include <isi_domain/dom.h>

time_t last_stat_time = 0;

/*  ____  _        _     _____                 _   _                 
 * / ___|| |_ __ _| |_  |  ___|   _ _ __   ___| |_(_) ___  _ __  ___ 
 * \___ \| __/ _` | __| | |_ | | | | '_ \ / __| __| |/ _ \| '_ \/ __|
 *  ___) | || (_| | |_  |  _|| |_| | | | | (__| |_| | (_) | | | \__ \
 * |____/ \__\__,_|\__| |_|   \__,_|_| |_|\___|\__|_|\___/|_| |_|___/
 */

void
init_statistics(struct siq_stat **stat)
{
	siq_stat_free(*stat);
	*stat = siq_stat_create();
}

#define STATDIFF(field) (cur_stats->field - last_stats->field)

static void
pworker_tw_stat_msg_pack(struct generic_msg *msg, struct siq_stat *cur_stats,
    struct siq_stat *last_stats, uint64_t dirlin, uint64_t filelin,
    uint32_t ver)
{
	struct old_pworker_tw_stat_msg *tss = &msg->body.old_pworker_tw_stat;
	struct siq_ript_msg *ript = NULL;

	if (ver < MSG_VERSION_HALFPIPE) {
		msg->head.type = OLD_PWORKER_TW_STAT_MSG;

		tss->dirs_visited = STATDIFF(dirs->src->visited);
		tss->dirs_dst_deleted = STATDIFF(dirs->dst->deleted);
		tss->files_total = STATDIFF(files->total);
		tss->files_replicated_selected =
		    STATDIFF(files->replicated->selected);
		tss->files_replicated_transferred =
		    STATDIFF(files->replicated->transferred);
		tss->files_replicated_new_files =
		    STATDIFF(files->replicated->new_files);
		tss->files_replicated_updated_files =
		    STATDIFF(files->replicated->updated_files);
		tss->files_replicated_files_with_ads =
		    STATDIFF(files->replicated->files_with_ads);
		tss->files_replicated_ads_streams =
		    STATDIFF(files->replicated->ads_streams);
		tss->files_replicated_symlinks =
		    STATDIFF(files->replicated->symlinks);
		tss->files_replicated_block_specs =
		    STATDIFF(files->replicated->block_specs);
		tss->files_replicated_char_specs =
		    STATDIFF(files->replicated->char_specs);
		tss->files_replicated_sockets =
		    STATDIFF(files->replicated->sockets);
		tss->files_replicated_fifos =
		    STATDIFF(files->replicated->fifos);
		tss->files_replicated_hard_links =
		    STATDIFF(files->replicated->hard_links);
		tss->files_deleted_src = STATDIFF(files->deleted->src);
		tss->files_deleted_dst = STATDIFF(files->deleted->dst);
		tss->files_skipped_up_to_date =
		    STATDIFF(files->skipped->up_to_date);
		tss->files_skipped_user_conflict =
		    STATDIFF(files->skipped->user_conflict);
		tss->files_skipped_error_io =
		    STATDIFF(files->skipped->error_io);
		tss->files_skipped_error_net =
		    STATDIFF(files->skipped->error_net);
		tss->files_skipped_error_checksum =
		    STATDIFF(files->skipped->error_checksum);
		tss->bytes_transferred = STATDIFF(bytes->transferred);
		tss->bytes_data_total = STATDIFF(bytes->data->total);
		tss->bytes_data_file = STATDIFF(bytes->data->file);
		tss->bytes_data_sparse = STATDIFF(bytes->data->sparse);
		tss->bytes_data_unchanged = STATDIFF(bytes->data->unchanged);
		tss->bytes_network_total = STATDIFF(bytes->network->total);
		tss->bytes_network_to_target =
		    STATDIFF(bytes->network->to_target);
		tss->bytes_network_to_source =
		    STATDIFF(bytes->network->to_source);

		/* send state of worker with stat */
		tss->twlin = dirlin;
		tss->filelin = filelin;

	} else {
		msg->head.type = build_ript_msg_type(PWORKER_TW_STAT_MSG);
		ript = &msg->body.ript;
		ript_msg_init(ript);

		ript_msg_set_field_uint32(ript, RMF_DIRS_VISITED, 1,
		    STATDIFF(dirs->src->visited));
		ript_msg_set_field_uint32(ript, RMF_DIRS_DELETED_DST, 1,
		    STATDIFF(dirs->dst->deleted));
		ript_msg_set_field_uint32(ript, RMF_FILES_TOTAL, 1,
		    STATDIFF(files->total));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_SELECTED,
		    1, STATDIFF(files->replicated->selected));
		ript_msg_set_field_uint32(ript,
		    RMF_FILES_REPLICATED_TRANSFERRED, 1,
		    STATDIFF(files->replicated->transferred));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_NEW_FILES,
		    1, STATDIFF(files->replicated->new_files));
		ript_msg_set_field_uint32(ript,
		    RMF_FILES_REPLICATED_UPDATED_FILES, 1,
		    STATDIFF(files->replicated->updated_files));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_WITH_ADS,
		    1, STATDIFF(files->replicated->files_with_ads));
		ript_msg_set_field_uint32(ript,
		    RMF_FILES_REPLICATED_ADS_STREAMS, 1,
		    STATDIFF(files->replicated->ads_streams));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_SYMLINKS,
		    1, STATDIFF(files->replicated->symlinks));
		ript_msg_set_field_uint32(ript,
		    RMF_FILES_REPLICATED_BLOCK_SPECS, 1,
		    STATDIFF(files->replicated->block_specs));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_CHAR_SPECS,
		    1, STATDIFF(files->replicated->char_specs));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_SOCKETS, 1,
		    STATDIFF(files->replicated->sockets));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_FIFOS, 1,
		    STATDIFF(files->replicated->fifos));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_HARD_LINKS,
		    1, STATDIFF(files->replicated->hard_links));
		ript_msg_set_field_uint32(ript, RMF_FILES_DELETED_SRC, 1,
		    STATDIFF(files->deleted->src));
		ript_msg_set_field_uint32(ript, RMF_FILES_DELETED_DST, 1,
		    STATDIFF(files->deleted->dst));
		ript_msg_set_field_uint32(ript, RMF_FILES_SKIPPED_UP_TO_DATE, 1,
		    STATDIFF(files->skipped->up_to_date));
		ript_msg_set_field_uint32(ript, RMF_FILES_SKIPPED_USER_CONFLICT,
		    1, STATDIFF(files->skipped->user_conflict));
		ript_msg_set_field_uint32(ript, RMF_FILES_SKIPPED_ERROR_IO, 1,
		    STATDIFF(files->skipped->error_io));
		ript_msg_set_field_uint32(ript, RMF_FILES_SKIPPED_ERROR_NET, 1,
		    STATDIFF(files->skipped->error_net));
		ript_msg_set_field_uint32(ript,
		    RMF_FILES_SKIPPED_ERROR_CHECKSUM, 1,
		    STATDIFF(files->skipped->error_checksum));
		ript_msg_set_field_uint64(ript, RMF_BYTES_TRANSFERRED, 1,
		    STATDIFF(bytes->transferred));
		ript_msg_set_field_uint64(ript, RMF_BYTES_DATA_TOTAL, 1,
		    STATDIFF(bytes->data->total));
		ript_msg_set_field_uint64(ript, RMF_BYTES_DATA_FILE, 1,
		    STATDIFF(bytes->data->file));
		ript_msg_set_field_uint64(ript, RMF_BYTES_DATA_SPARSE, 1,
		    STATDIFF(bytes->data->sparse));
		ript_msg_set_field_uint64(ript, RMF_BYTES_DATA_UNCHANGED, 1,
		    STATDIFF(bytes->data->unchanged));
		ript_msg_set_field_uint64(ript, RMF_BYTES_NETWORK_TOTAL, 1,
		    STATDIFF(bytes->network->total));
		ript_msg_set_field_uint64(ript, RMF_BYTES_NETWORK_TO_TARGET, 1,
		    STATDIFF(bytes->network->to_target));
		ript_msg_set_field_uint64(ript, RMF_BYTES_NETWORK_TO_SOURCE, 1,
		    STATDIFF(bytes->network->to_source));

		/* send state of worker with stat */
		ript_msg_set_field_uint64(ript, RMF_TWLIN, 1, dirlin);
		ript_msg_set_field_uint64(ript, RMF_FILELIN, 1, filelin);
	}

	siq_stat_copy(last_stats, cur_stats);
}

static void
sworker_tw_stat_msg_pack(struct generic_msg *msg, struct siq_stat *cur_stats,
    struct siq_stat *last_stats, uint32_t ver)
{
	struct old_sworker_tw_stat_msg *tss = &msg->body.old_sworker_tw_stat;
	struct siq_ript_msg *ript = NULL;

	if (ver < MSG_VERSION_HALFPIPE) {
		msg->head.type = OLD_SWORKER_TW_STAT_MSG;

		tss->dirs_visited = STATDIFF(dirs->src->visited);
		tss->dirs_dst_deleted = STATDIFF(dirs->dst->deleted);
		tss->files_total = STATDIFF(files->total);
		tss->files_replicated_selected =
		    STATDIFF(files->replicated->selected);
		tss->files_replicated_transferred =
		    STATDIFF(files->replicated->transferred);
		tss->files_deleted_src = STATDIFF(files->deleted->src);
		tss->files_deleted_dst = STATDIFF(files->deleted->dst);
		tss->files_skipped_up_to_date =
		    STATDIFF(files->skipped->up_to_date);
		tss->files_skipped_user_conflict =
		    STATDIFF(files->skipped->user_conflict);
		tss->files_skipped_error_io =
		    STATDIFF(files->skipped->error_io);
		tss->files_skipped_error_net =
		    STATDIFF(files->skipped->error_net);
		tss->files_skipped_error_checksum =
		    STATDIFF(files->skipped->error_checksum);
	} else {
		msg->head.type = build_ript_msg_type(SWORKER_TW_STAT_MSG);
		ript = &msg->body.ript;
		ript_msg_init(ript);

		ript_msg_set_field_uint32(ript, RMF_DIRS_VISITED, 1,
		    STATDIFF(dirs->src->visited));
		ript_msg_set_field_uint32(ript, RMF_DIRS_DELETED_DST, 1,
		    STATDIFF(dirs->dst->deleted));
		ript_msg_set_field_uint32(ript, RMF_FILES_TOTAL, 1,
		    STATDIFF(files->total));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_SELECTED,
		    1, STATDIFF(files->replicated->selected));
		ript_msg_set_field_uint32(ript,
		    RMF_FILES_REPLICATED_TRANSFERRED, 1,
		    STATDIFF(files->replicated->transferred));
		ript_msg_set_field_uint32(ript, RMF_FILES_DELETED_SRC, 1,
		    STATDIFF(files->deleted->src));
		ript_msg_set_field_uint32(ript, RMF_FILES_DELETED_DST, 1,
		    STATDIFF(files->deleted->dst));
		ript_msg_set_field_uint32(ript, RMF_FILES_SKIPPED_UP_TO_DATE, 1,
		    STATDIFF(files->skipped->up_to_date));
		ript_msg_set_field_uint32(ript, RMF_FILES_SKIPPED_USER_CONFLICT,
		    1, STATDIFF(files->skipped->user_conflict));
		ript_msg_set_field_uint32(ript, RMF_FILES_SKIPPED_ERROR_IO, 1,
		    STATDIFF(files->skipped->error_io));
		ript_msg_set_field_uint32(ript, RMF_FILES_SKIPPED_ERROR_NET, 1,
		    STATDIFF(files->skipped->error_net));
		ript_msg_set_field_uint32(ript,
		    RMF_FILES_SKIPPED_ERROR_CHECKSUM, 1,
		    STATDIFF(files->skipped->error_checksum));
		ript_msg_set_field_uint32(ript, RMF_COMP_COMMITTED_FILES, 1,
		    STATDIFF(compliance->committed_files));
	}

	siq_stat_copy(last_stats, cur_stats);
}

static void
pworker_stf_stat_msg_pack(struct generic_msg *msg, struct siq_stat *cur_stats,
    struct siq_stat *last_stats, uint32_t ver)
{
	struct old_pworker_stf_stat_msg *tss = &msg->body.old_pworker_stf_stat;
	struct siq_ript_msg *ript = NULL;

	if (ver < MSG_VERSION_HALFPIPE) {
		msg->head.type = OLD_PWORKER_STF_STAT_MSG;

		tss->dirs_created_src = STATDIFF(dirs->created->src);
		tss->dirs_deleted_src = STATDIFF(dirs->deleted->src);
		tss->dirs_linked_src = STATDIFF(dirs->linked->src);
		tss->dirs_unlinked_src = STATDIFF(dirs->unlinked->src);
		tss->dirs_replicated = STATDIFF(dirs->replicated);
		tss->files_total = STATDIFF(files->total);
		tss->files_replicated_new_files =
		    STATDIFF(files->replicated->new_files);
		tss->files_replicated_updated_files =
		    STATDIFF(files->replicated->updated_files);
		tss->files_replicated_regular =
		    STATDIFF(files->replicated->regular);
		tss->files_replicated_with_ads =
		    STATDIFF(files->replicated->files_with_ads);
		tss->files_replicated_ads_streams =
		    STATDIFF(files->replicated->ads_streams);
		tss->files_replicated_symlinks =
		    STATDIFF(files->replicated->symlinks);
		tss->files_replicated_block_specs =
		    STATDIFF(files->replicated->block_specs);
		tss->files_replicated_char_specs =
		    STATDIFF(files->replicated->char_specs);
		tss->files_replicated_sockets =
		    STATDIFF(files->replicated->sockets);
		tss->files_replicated_fifos =
		    STATDIFF(files->replicated->fifos);
		tss->files_replicated_hard_links =
		    STATDIFF(files->replicated->hard_links);
		tss->files_deleted_src = STATDIFF(files->deleted->src);
		tss->files_linked_src = STATDIFF(files->linked->src);
		tss->files_unlinked_src = STATDIFF(files->unlinked->src);
		tss->bytes_data_total = STATDIFF(bytes->data->total);
		tss->bytes_data_file = STATDIFF(bytes->data->file);
		tss->bytes_data_file = STATDIFF(bytes->data->file);
		tss->bytes_data_sparse = STATDIFF(bytes->data->sparse);
		tss->bytes_network_total = STATDIFF(bytes->network->total);
		tss->bytes_network_to_target =
		    STATDIFF(bytes->network->to_target);
		tss->bytes_network_to_source =
		    STATDIFF(bytes->network->to_source);
		tss->cc_lins_total = STATDIFF(change_compute->lins_total);
		tss->cc_dirs_new = STATDIFF(change_compute->dirs_new);
		tss->cc_dirs_deleted = STATDIFF(change_compute->dirs_deleted);
		tss->cc_dirs_moved = STATDIFF(change_compute->dirs_moved);
		tss->cc_dirs_changed = STATDIFF(change_compute->dirs_changed);
		tss->cc_files_new = STATDIFF(change_compute->files_new);
		tss->cc_files_linked = STATDIFF(change_compute->files_linked);
		tss->cc_files_unlinked =
		    STATDIFF(change_compute->files_unlinked);
		tss->cc_files_changed = STATDIFF(change_compute->files_changed);
		tss->ct_hash_exceptions_found =
		    STATDIFF(change_transfer->hash_exceptions_found);
		tss->ct_hash_exceptions_fixed =
		    STATDIFF(change_transfer->hash_exceptions_fixed);
		tss->ct_flipped_lins = STATDIFF(change_transfer->flipped_lins);
		tss->ct_corrected_lins =
		    STATDIFF(change_transfer->corrected_lins);
		tss->ct_resynced_lins =
		    STATDIFF(change_transfer->resynced_lins);
	} else {
		msg->head.type = build_ript_msg_type(PWORKER_STF_STAT_MSG);
		ript = &msg->body.ript;
		ript_msg_init(ript);

		ript_msg_set_field_uint32(ript, RMF_DIRS_CREATED_SRC, 1,
		    STATDIFF(dirs->created->src));
		ript_msg_set_field_uint32(ript, RMF_DIRS_DELETED_SRC, 1,
		    STATDIFF(dirs->deleted->src));
		ript_msg_set_field_uint32(ript, RMF_DIRS_LINKED_SRC, 1,
		    STATDIFF(dirs->linked->src));
		ript_msg_set_field_uint32(ript, RMF_DIRS_UNLINKED_SRC, 1,
		    STATDIFF(dirs->unlinked->src));
		ript_msg_set_field_uint32(ript, RMF_DIRS_REPLICATED, 1,
		    STATDIFF(dirs->replicated));
		ript_msg_set_field_uint32(ript, RMF_FILES_TOTAL, 1,
		    STATDIFF(files->total));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_NEW_FILES,
		    1, STATDIFF(files->replicated->new_files));
		ript_msg_set_field_uint32(ript,
		    RMF_FILES_REPLICATED_UPDATED_FILES, 1,
		    STATDIFF(files->replicated->updated_files));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_REGULAR, 1,
		    STATDIFF(files->replicated->regular));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_WITH_ADS,
		    1, STATDIFF(files->replicated->files_with_ads));
		ript_msg_set_field_uint32(ript,
		    RMF_FILES_REPLICATED_ADS_STREAMS, 1,
		    STATDIFF(files->replicated->ads_streams));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_SYMLINKS,
		    1, STATDIFF(files->replicated->symlinks));
		ript_msg_set_field_uint32(ript,
		    RMF_FILES_REPLICATED_BLOCK_SPECS, 1,
		    STATDIFF(files->replicated->block_specs));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_CHAR_SPECS,
		    1, STATDIFF(files->replicated->char_specs));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_SOCKETS, 1,
		    STATDIFF(files->replicated->sockets));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_FIFOS, 1,
		    STATDIFF(files->replicated->fifos));
		ript_msg_set_field_uint32(ript, RMF_FILES_REPLICATED_HARD_LINKS,
		    1, STATDIFF(files->replicated->hard_links));
		ript_msg_set_field_uint32(ript, RMF_FILES_DELETED_SRC, 1,
		    STATDIFF(files->deleted->src));
		ript_msg_set_field_uint32(ript, RMF_FILES_LINKED_SRC, 1,
		    STATDIFF(files->linked->src));
		ript_msg_set_field_uint32(ript, RMF_FILES_UNLINKED_SRC, 1,
		    STATDIFF(files->unlinked->src));
		ript_msg_set_field_uint64(ript, RMF_BYTES_DATA_TOTAL, 1,
		    STATDIFF(bytes->data->total));
		ript_msg_set_field_uint64(ript, RMF_BYTES_DATA_FILE, 1,
		    STATDIFF(bytes->data->file));
		ript_msg_set_field_uint64(ript, RMF_BYTES_DATA_FILE, 1,
		    STATDIFF(bytes->data->file));
		ript_msg_set_field_uint64(ript, RMF_BYTES_DATA_SPARSE, 1,
		    STATDIFF(bytes->data->sparse));
		ript_msg_set_field_uint64(ript, RMF_BYTES_DATA_UNCHANGED, 1,
		    STATDIFF(bytes->data->unchanged));
		ript_msg_set_field_uint64(ript, RMF_BYTES_NETWORK_TOTAL, 1,
		    STATDIFF(bytes->network->total));
		ript_msg_set_field_uint64(ript, RMF_BYTES_NETWORK_TO_TARGET, 1,
		    STATDIFF(bytes->network->to_target));
		ript_msg_set_field_uint64(ript, RMF_BYTES_NETWORK_TO_SOURCE, 1,
		    STATDIFF(bytes->network->to_source));
		ript_msg_set_field_uint32(ript, RMF_CC_LINS_TOTAL, 1,
		    STATDIFF(change_compute->lins_total));
		ript_msg_set_field_uint32(ript, RMF_CC_DIRS_NEW, 1,
		    STATDIFF(change_compute->dirs_new));
		ript_msg_set_field_uint32(ript, RMF_CC_DIRS_DELETED, 1,
		    STATDIFF(change_compute->dirs_deleted));
		ript_msg_set_field_uint32(ript, RMF_CC_DIRS_MOVED, 1,
		    STATDIFF(change_compute->dirs_moved));
		ript_msg_set_field_uint32(ript, RMF_CC_DIRS_CHANGED, 1,
		    STATDIFF(change_compute->dirs_changed));
		ript_msg_set_field_uint32(ript, RMF_CC_FILES_NEW, 1,
		    STATDIFF(change_compute->files_new));
		ript_msg_set_field_uint32(ript, RMF_CC_FILES_LINKED, 1,
		    STATDIFF(change_compute->files_linked));
		ript_msg_set_field_uint32(ript, RMF_CC_FILES_UNLINKED, 1,
		    STATDIFF(change_compute->files_unlinked));
		ript_msg_set_field_uint32(ript, RMF_CC_FILES_CHANGED, 1,
		    STATDIFF(change_compute->files_changed));
		ript_msg_set_field_uint32(ript, RMF_CT_HASH_EXCEPTIONS_FOUND, 1,
		    STATDIFF(change_transfer->hash_exceptions_found));
		ript_msg_set_field_uint32(ript, RMF_CT_HASH_EXCEPTIONS_FIXED, 1,
		    STATDIFF(change_transfer->hash_exceptions_fixed));
		ript_msg_set_field_uint32(ript, RMF_CT_FLIPPED_LINS, 1,
		    STATDIFF(change_transfer->flipped_lins));
		ript_msg_set_field_uint32(ript, RMF_CT_CORRECTED_LINS, 1,
		    STATDIFF(change_transfer->corrected_lins));
		ript_msg_set_field_uint32(ript, RMF_CT_RESYNCED_LINS, 1,
		    STATDIFF(change_transfer->resynced_lins));

		ript_msg_set_field_uint32(ript, RMF_RESYNC_COMPLIANCE_DIRS_NEW,
		    1, STATDIFF(compliance->resync_compliance_dirs_new));
		ript_msg_set_field_uint32(ript,
		    RMF_RESYNC_COMPLIANCE_DIRS_LINKED, 1,
		    STATDIFF(compliance->resync_compliance_dirs_linked));
		ript_msg_set_field_uint32(ript, RMF_RESYNC_CONFLICT_FILES_NEW,
		    1, STATDIFF(compliance->resync_conflict_files_new));
		ript_msg_set_field_uint32(ript,
		    RMF_RESYNC_CONFLICT_FILES_LINKED, 1,
		    STATDIFF(compliance->resync_conflict_files_linked));
	}

	siq_stat_copy(last_stats, cur_stats);
}

static void
sworker_stf_stat_msg_pack(struct generic_msg *msg, struct siq_stat *cur_stats,
    struct siq_stat *last_stats, uint32_t ver)
{
	struct old_sworker_stf_stat_msg *tss = &msg->body.old_sworker_stf_stat;
	struct siq_ript_msg *ript = NULL;

	if (ver < MSG_VERSION_HALFPIPE) {
		msg->head.type = OLD_SWORKER_STF_STAT_MSG;

		tss->dirs_created_dst = STATDIFF(dirs->created->dst);
		tss->dirs_deleted_dst = STATDIFF(dirs->deleted->dst);
		tss->dirs_linked_dst = STATDIFF(dirs->linked->dst);
		tss->dirs_unlinked_dst = STATDIFF(dirs->unlinked->dst);
		tss->files_deleted_dst = STATDIFF(files->deleted->dst);
		tss->files_linked_dst = STATDIFF(files->linked->dst);
		tss->files_unlinked_dst = STATDIFF(files->unlinked->dst);
	} else {
		msg->head.type = build_ript_msg_type(SWORKER_STF_STAT_MSG);
		ript = &msg->body.ript;
		ript_msg_init(ript);

		ript_msg_set_field_uint32(ript, RMF_DIRS_CREATED_DST, 1,
		    STATDIFF(dirs->created->dst));
		ript_msg_set_field_uint32(ript, RMF_DIRS_DELETED_DST, 1,
		    STATDIFF(dirs->deleted->dst));
		ript_msg_set_field_uint32(ript, RMF_DIRS_LINKED_DST, 1,
		    STATDIFF(dirs->linked->dst));
		ript_msg_set_field_uint32(ript, RMF_DIRS_UNLINKED_DST, 1,
		    STATDIFF(dirs->unlinked->dst));
		ript_msg_set_field_uint32(ript, RMF_FILES_DELETED_DST, 1,
		    STATDIFF(files->deleted->dst));
		ript_msg_set_field_uint32(ript, RMF_FILES_LINKED_DST, 1,
		    STATDIFF(files->linked->dst));
		ript_msg_set_field_uint32(ript, RMF_FILES_UNLINKED_DST, 1,
		    STATDIFF(files->unlinked->dst));
		ript_msg_set_field_uint32(ript, RMF_COMPLIANCE_CONFLICTS, 1,
		    STATDIFF(compliance->conflicts));
		ript_msg_set_field_uint32(ript, RMF_COMP_COMMITTED_FILES, 1,
		    STATDIFF(compliance->committed_files));
	}

	siq_stat_copy(last_stats, cur_stats);
}

void
send_pworker_tw_stat(int fd, struct siq_stat *cur_stats,
    struct siq_stat *last_stats, uint64_t dirlin, uint64_t filelin,
    uint32_t ver)
{
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);

	pworker_tw_stat_msg_pack(&msg, cur_stats, last_stats, dirlin, filelin,
	    ver);
	msg_send(fd, &msg);
}

void
send_sworker_tw_stat(int fd, struct siq_stat *cur_stats,
    struct siq_stat *last_stats, uint32_t ver)
{
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);

	sworker_tw_stat_msg_pack(&msg, cur_stats, last_stats, ver);
	msg_send(fd, &msg);
}

void
send_pworker_stf_stat(int fd, struct siq_stat *cur_stats,
    struct siq_stat *last_stats, uint32_t ver)
{
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);

	pworker_stf_stat_msg_pack(&msg, cur_stats, last_stats, ver);
	msg_send(fd, &msg);
}

void
send_sworker_stf_stat(int fd, struct siq_stat *cur_stats,
    struct siq_stat *last_stats, uint32_t ver)
{
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);

	sworker_stf_stat_msg_pack(&msg, cur_stats, last_stats, ver);
	msg_send(fd, &msg);
}

#undef STATDIFF

/*Error functions*/
#define SEND_ERR(err) do {						\
	m.body.error.error = err;					\
	m.head.type = ERROR_MSG;					\
	m.body.error.io = 1;						\
	va_start(ap, fmt);						\
	vasprintf(&m.body.error.str, fmt, ap);				\
	va_end(ap);							\
									\
	log(TRACE, "Error with errno %d",				\
		m.body.error.error);					\
	msg_send(s, &m);						\
} while (0)

#define SEND_POSITION() do {						\
	if (dir != NULL) {	\
		m.head.type = POSITION_MSG;				\
		m.body.position.reason = POSITION_ERROR;		\
		m.body.position.comment = strerror(err);		\
		m.body.position.file = file;				\
		m.body.position.enc = enc;				\
		m.body.position.filelin = objectlin;			\
		m.body.position.dir = dir;				\
		m.body.position.eveclen = 0;				\
		m.body.position.evec = NULL;				\
		m.body.position.dirlin = 0;				\
		msg_send(s, &m);					\
	}								\
} while(0)

void
ioerror(int s, char* dir, char* file, enc_t enc, uint64_t objectlin,
    char *fmt, ...)
{
	va_list ap;
	struct generic_msg m;
	int err;

	err = errno;
	if((errno == EROFS) || (errno == ENOSPC)){ /*Fatal errors*/
		SEND_ERR(err);
		log(FATAL, "%s: %s (lin %llx)",
			 m.body.error.str, strerror(err), objectlin);
		free(m.body.error.str);	
	} else if (errno == ENXIO || err == ENODEV || err == EBUSY){
		/*Fatal only for this worker*/
		SEND_POSITION();
		SEND_ERR(err);
		exit(EXIT_FAILURE);
	} else {
		SEND_POSITION();
		SEND_ERR(err);/*do nothing*/
		/*Go through*/
	}
}

void
cluster_name_from_node_name(char *node_name)
{
	int i;

	/* Convert a node's hostname to the cluster name. Usually, the
	 * hostname is xxx-n, where xxx is the cluster name and n is the lnn
	 * of the node. Some customers have been known to drop the '-' from
	 * the hostname, so we find the cluster name by scanning the hostname
	 * backwards and stopping at a '-' or non-digit. Also handle the case
	 * where the cluster name includes a dash and number, e.g. "xxx-1"
	 * with node names "xxx-1-n" */
	for (i = strlen(node_name) - 1; i >= 0; i--) {
		if (isdigit(node_name[i]))
			node_name[i] = '\0';
		else if (node_name[i] == '-') {
			node_name[i] = '\0';
			break;
		}
		else
			break;
	}
}

bool
check_for_dont_run(bool do_sleep)
{
	if (access(DONT_RUN_FILE, R_OK) == 0) {
		log(NOTICE,
		    "This node has been marked to have all SIQ executables\n"
		    "to be in sleep mode and do nothing (%s).",
		    DONT_RUN_FILE);
		if (!do_sleep)
			return true;
		/* Display "sleep mode" when ps is run */
		setproctitle("sleep mode");
		for (;;) {
			if (access(DONT_RUN_FILE, R_OK) != 0) {
				log(NOTICE,
				    "This process is no longer in sleep mode");
				/* Restore original process title */
				setproctitle(NULL);
				break;
			}	    
			siq_nanosleep(1, 0);
		}
	}
	return false;
}

static void
send_comp_error(int fd, char *error_msg, enum siqerrors siq_error,
    int errloc, struct isi_error *error)
{
	char *isi_error_msg = NULL;
	struct isi_error *error_to_send = NULL;

	isi_error_msg = isi_error_get_detailed_message(error);
	log(ERROR, "%s: %s", error_msg, isi_error_msg);

	error_to_send = isi_siq_error_new(siq_error, "%s: %s", error_msg,
	    isi_error_msg);
	siq_send_error(fd, (struct isi_siq_error *)error_to_send, errloc);

	isi_error_free(error_to_send);
	free(isi_error_msg);
}

void
send_comp_source_error(int fd, char *error_msg, enum siqerrors siq_error,
    struct isi_error *error)
{
	send_comp_error(fd, error_msg, siq_error, EL_SIQ_SOURCE, error);
}

void
send_comp_target_error(int fd, char *error_msg, enum siqerrors siq_error,
    struct isi_error *error)
{
	send_comp_error(fd, error_msg, siq_error, EL_SIQ_DEST, error);
}

void
send_compliance_map_entries(int fd, uint32_t num_entries,
    struct lmap_update *entries, ifs_lin_t chkpt_lin)
{
	uint32_t buf_len;
	struct generic_msg GEN_MSG_INIT_CLEAN(msg);
	struct siq_ript_msg *ript = &msg.body.ript;

	buf_len = num_entries * sizeof(*entries);

	msg.head.type = build_ript_msg_type(COMP_MAP_MSG);
	ript_msg_init(ript);
	ript_msg_set_field_uint32(ript, RMF_SIZE, 1, num_entries);
	ript_msg_set_field_bytestream(ript, RMF_COMP_MAP_BUF, 1,
	    (uint8_t *)entries, buf_len);
	ript_msg_set_field_uint64(ript, RMF_CHKPT_LIN, 1, chkpt_lin);

	msg_send(fd, &msg);
}
