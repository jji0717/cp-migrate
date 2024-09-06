
#include "isi_cbm_mapinfo_store_sbt.h"
#include "isi_cbm_mapper.h"

#include <vector>
#include <string.h>
#include <sstream>
#include <isi_util/isi_hash.h>
#include <isi_ilog/ilog.h>
#include <isi_sbtree/sbtree.h>

namespace {

const char SBT_MAPINFO_DIR[] = "cloudpools_mapinfo";

enum { HASH_LEVEL = 1024 };

std::string
get_sbt_suffix(isi_cfm_mapinfo_store_cb *cb)
{
	ASSERT(cb);
	return cb->get_store_suffix();
}

}

isi_cfm_mapinfo_store_sbt::isi_cfm_mapinfo_store_sbt(
    isi_cfm_mapinfo_store_cb *cb)
    : sbt_fd_(-1), cb_(cb)
{
	ASSERT(cb_);
}

isi_cfm_mapinfo_store_sbt::~isi_cfm_mapinfo_store_sbt()
{
	if (sbt_fd_ >= 0)
		close(sbt_fd_);
}

void
isi_cfm_mapinfo_store_sbt::move(struct isi_error **error_out)
{
	struct stat st;
	int parent_fd = -1, new_parent_fd = -1;
	struct isi_error *error = NULL;
	std::string new_fname = cb_->get_store_name();
	std::string new_suffix = get_sbt_suffix(cb_);
	std::string cur_fname_suffix = sbt_fname_ + sbt_suffix_;
	std::string new_fname_suffix = new_fname + new_suffix;

	if (sbt_fd_ < 0 || (cur_fname_suffix == new_fname_suffix))
		goto out;

	// open folders (derived from name only)
	parent_fd = open_folder(sbt_fname_, true, &error);
	ON_ISI_ERROR_GOTO(out, error, "failed to open sbt folder");

	new_parent_fd = open_folder(new_fname, true, &error);
	ON_ISI_ERROR_GOTO(out, error, "failed to open sbt folder");

	if (fstat(sbt_fd_, &st) != 0) {
		error = isi_system_error_new(errno,
		    "failed to move store: %s to %s",
		    sbt_fname_.c_str(), new_fname.c_str());

		goto out;
	}

	// create a new link name, unlink it in case it exists
	enc_unlinkat(new_parent_fd, new_fname_suffix.c_str(), ENC_DEFAULT, 0);

	if (enc_flinkat(sbt_fd_, new_parent_fd,
	    new_fname_suffix.c_str(), ENC_DEFAULT) != 0) {
		error = isi_system_error_new(errno,
		    "failed to move store: %s to %s",
		    cur_fname_suffix.c_str(), new_fname_suffix.c_str());

		goto out;
	}

	// remove the original link name
	if (enc_unlinkat(parent_fd, cur_fname_suffix.c_str(), 
	    ENC_DEFAULT, 0) != 0) {
		error = isi_system_error_new(errno,
		    "failed to move store: %s to %s",
		    cur_fname_suffix.c_str(), new_fname_suffix.c_str());

		goto out;
	}

	ilog(IL_DEBUG, "move store %s - %s", cur_fname_suffix.c_str(), 
	    new_fname_suffix.c_str());

	sbt_fname_ = new_fname;
	sbt_suffix_ = new_suffix;

 out:
	if (parent_fd >= 0)
		close(parent_fd);

	if (new_parent_fd >= 0)
		close(new_parent_fd);

	isi_error_handle(error, error_out);
}

void
isi_cfm_mapinfo_store_sbt::open(bool create_if_not_found,
    bool overwrite_if_exist, bool *created,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (created) {
		*created = false;
	}

	if (sbt_fd_ >= 0) {
		close(sbt_fd_);
		sbt_fd_ = -1;
	}

	// <SBT_ROOT>/cloudpools_mapinfo/<hash_l1>/<hash_l2>/<fname><suffix>
	int parent_fd = -1;
	sbt_fname_ = cb_->get_store_name();
	sbt_suffix_ = get_sbt_suffix(cb_);
	std::string sbt_fname_suffix = sbt_fname_ + sbt_suffix_;

	// open folders (based on sbt name alone)
	parent_fd  = open_folder(sbt_fname_, create_if_not_found, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (overwrite_if_exist) {
		// make sure it is deleted first
		int ret = enc_unlinkat(parent_fd, sbt_fname_suffix.c_str(),
		    ENC_DEFAULT, 0);

		if (ret == 0) {
			ilog(IL_DEBUG, "Deleted store %s. It will be recreated.",
			    sbt_fname_suffix.c_str());
		}
	}
	// open sbt
	sbt_fd_ = enc_openat(parent_fd, sbt_fname_suffix.c_str(),
	    ENC_DEFAULT, O_RDWR);

	// create sbt if failed on open
	if (sbt_fd_ < 0 && errno == ENOENT && create_if_not_found) {
		int res = ifs_sbt_create(parent_fd, sbt_fname_suffix.c_str(),
		    ENC_DEFAULT, SBT_128BIT);

		if (res == 0) {
			sbt_fd_ = enc_openat(parent_fd, sbt_fname_suffix.c_str(),
			    ENC_DEFAULT, O_RDWR);

			if (sbt_fd_ >= 0) {
				if (created) {
					*created = true;
				}
				ilog(IL_DEBUG, "created store %s",
				    sbt_fname_suffix.c_str());
			}
		}
	}

	if (sbt_fd_ < 0) {
		error = isi_system_error_new(errno, "failed to open/create"
		    " sbt for mapinfo: %s", sbt_fname_suffix.c_str());
		goto out;
	}

 out:
	if (parent_fd >= 0)
		close(parent_fd);

	isi_error_handle(error, error_out);
}


void
isi_cfm_mapinfo_store_sbt::put(off_t off, isi_cfm_mapentry &entry,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	btree_key_t key = {{off, 0}};
	btree_flags flags = {};
	int res = 0;

	if (sbt_fd_ < 0) {
		error = isi_system_error_new(EINVAL,
		    "sbt file was not open: %s",
		    cb_->get_store_name().c_str());
		goto out;
	}

	// true: not to pack in extended attributes but pack in sbt.
	entry.pack(cb_->get_type(), true);

	res = ifs_sbt_add_entry(sbt_fd_, &key, entry.get_packed_entry(),
	    entry.get_packed_size(), &flags);

	entry.free_packed_entry();

	if (res != 0) {
		error = isi_system_error_new(errno,
		    "failed to add sbt entry: %s",
		    cb_->get_store_name().c_str());
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

void
isi_cfm_mapinfo_store_sbt::put(isi_cfm_mapentry entries[], int cnt,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct btree_flags flags = {};
	int page = 0;

	if (sbt_fd_ < 0) {
		error = isi_system_error_new(EINVAL,
		    "sbt file was not open: %s",
		    cb_->get_store_name().c_str());
		goto out;
	}

	while (page <= cnt / SBT_BULK_MAX_OPS) {
		int num_ops =
		    MIN(cnt - page * SBT_BULK_MAX_OPS, SBT_BULK_MAX_OPS);
		int res = 0;
		struct sbt_bulk_entry sbt_ops[num_ops];

		if (num_ops == 0)
			break;

		for (int i = 0; i < num_ops; i++) {
			int idx = page * SBT_BULK_MAX_OPS + i;

			entries[idx].pack(cb_->get_type(), true);

			sbt_ops[i].fd = sbt_fd_;
			sbt_ops[i].op_type = SBT_SYS_OP_ADD;
			sbt_ops[i].key.keys[0] = entries[idx].get_offset();
			sbt_ops[i].key.keys[1] = 0;
			sbt_ops[i].entry_buf = entries[idx].get_packed_entry();
			sbt_ops[i].entry_size = entries[idx].get_packed_size();
			sbt_ops[i].entry_flags = flags;
			sbt_ops[i].cm = BT_CM_NONE;
			sbt_ops[i].old_entry_buf = NULL;
			sbt_ops[i].old_entry_size = 0;
			sbt_ops[i].old_entry_flags = flags;
		}

		res = ifs_sbt_bulk_op(num_ops, sbt_ops);

		if (res != 0) {
			error = isi_system_error_new(errno,
			    "failed to bulk_op %d entries to sbt: %s",
			    num_ops, cb_->get_store_name().c_str());
			goto out;
		}

		page++;
	}

 out:
	isi_error_handle(error, error_out);
}

bool
isi_cfm_mapinfo_store_sbt::get(off_t &off, isi_cfm_mapentry &entry, int opt,
    struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	btree_key_t key = {{off, 0}};
	btree_key_t entry_key;
	btree_flags flags = {};
	// true so not to include the offset
	size_t sz = cb_->get_mapentry_pack_size(true);
	std::vector<char> buf(sz);
	int res = -1;

	if (sbt_fd_ < 0) {
		error = isi_system_error_new(EINVAL,
		    "sbt file was not open: %s",
		    cb_->get_store_name().c_str());
		goto out;
	}

	switch (opt) {
	case isi_cfm_mapinfo_store::AT:
		res = ifs_sbt_get_entry_at(sbt_fd_, &key, buf.data(), sz, &flags,
		    &sz);

		if (res == 0) {
			// true: offset not packed
			entry.unpack(buf.data(), cb_->get_type(), &error, true);
			ON_ISI_ERROR_GOTO(out, error);
			entry.set_offset(off);
		}
		break;

	case isi_cfm_mapinfo_store::AT_OR_BEFORE:
		res = ifs_sbt_get_entry_at_or_before(sbt_fd_, &key, &entry_key,
		    buf.data(), sz, &flags, &sz);

		if (res == 0) {
			off = entry_key.keys[0];

			// true: offset not packed
			entry.unpack(buf.data(), cb_->get_type(), &error, true);
			ON_ISI_ERROR_GOTO(out, error);
			entry.set_offset(entry_key.keys[0]);
		}
		break;

	case isi_cfm_mapinfo_store::AT_OR_AFTER:
		res = ifs_sbt_get_entry_at_or_after(sbt_fd_, &key, &entry_key,
		    buf.data(), sz, &flags, &sz);

		if (res == 0) {
			off = entry_key.keys[0];
			// true: offset not packed
			entry.unpack(buf.data(), cb_->get_type(), &error, true);
			ON_ISI_ERROR_GOTO(out, error);
			entry.set_offset(off);
		}
		break;

	default:
		error = isi_system_error_new(EINVAL, "invalid option: %d", opt);
		break;
	}

 out:
	// ENOENT: not an error
	if (res != 0 && !error && errno != ENOENT) {
		error = isi_system_error_new(errno, "failed to get sbt entry: %s",
		    cb_->get_store_name().c_str());
	}

	isi_error_handle(error, error_out);

	return res == 0;
}

int
isi_cfm_mapinfo_store_sbt::get(off_t off, isi_cfm_mapentry entries[], int cnt,
    struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	btree_key_t key = {{off, 0}};
	btree_key_t next_key;
	// true so not to include the offset
	size_t sz = cnt *
	    (cb_->get_mapentry_pack_size(true) + sizeof(struct sbt_entry));
	std::vector<char> buf(sz);
	struct sbt_entry *ent = (struct sbt_entry *) buf.data();
	size_t ret_cnt = 0;
	int res = 0;

	if (sbt_fd_ < 0) {
		error = isi_system_error_new(EINVAL,
		    "sbt file was not open: %s",
		    cb_->get_store_name().c_str());
		goto out;
	}

	res = ifs_sbt_get_entries(sbt_fd_, &key, &next_key, sz, buf.data(),
	    cnt, &ret_cnt);

	if (res != 0) {
		error = isi_system_error_new(errno,
		    "failed to get sbt entries: %s",
		    cb_->get_store_name().c_str());
		goto out;
	}

	for (size_t i = 0; i < ret_cnt; i++) {
		// true: offset not packed
		entries[i].unpack(ent->buf, cb_->get_type(), &error, true);
		ON_ISI_ERROR_GOTO(out, error);
		entries[i].set_offset(ent->key.keys[0]);

		ent = (struct sbt_entry *) (ent->buf + ent->size_out);
	}

 out:
	isi_error_handle(error, error_out);

	return (int)ret_cnt;
}

void
isi_cfm_mapinfo_store_sbt::remove(off_t off, struct isi_error **error_out)
{
	// remove all the entries when off is negative
	if (off < 0)
	    remove_all(error_out);
	else
	    remove_at(off, error_out);
}

void
isi_cfm_mapinfo_store_sbt::remove(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int res = -1;
	std::string sbt_fname_suffix;
	// <SBT_ROOT>/cloudpools_mapinfo/fname
	int parent_fd = -1;

	if (sbt_fd_ >= 0) {
		close(sbt_fd_);
		sbt_fd_ = -1;
	}
	else
		goto out;

	sbt_fname_ = cb_->get_store_name();
	sbt_suffix_ = get_sbt_suffix(cb_);
	sbt_fname_suffix = sbt_fname_ + sbt_suffix_;

	parent_fd = open_folder(sbt_fname_, false, &error);
	ON_ISI_ERROR_GOTO(out, error);

	res = enc_unlinkat(parent_fd,
	    sbt_fname_suffix.c_str(), ENC_DEFAULT, 0);

	if (res != 0 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "failed to remove sbt file: %s",
			sbt_fname_suffix.c_str());
		goto out;
	}

 out:
	if (parent_fd >= 0)
		close(parent_fd);

	isi_error_handle(error, error_out);
}

int
isi_cfm_mapinfo_store_sbt::copy_from(isi_cfm_mapinfo_store &src,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	off_t offset = 0;
	int total_cnt = 0, cnt = 0;

	do {
		isi_cfm_mapentry entries[SBT_BULK_MAX_OPS];

		// bulk loading
		cnt = src.get(offset, entries, SBT_BULK_MAX_OPS, &error);

		ON_ISI_ERROR_GOTO(out, error, "failed to get sbt entry");

		// bulk insertion
		this->put(entries, cnt, &error);

		ON_ISI_ERROR_GOTO(out, error, "failed to put sbt entry");

		// move to next offset
		if (cnt > 0) {
			offset = entries[cnt - 1].get_offset() +
			    entries[cnt - 1].get_length();
		}

		total_cnt += cnt;
	} while (cnt == SBT_BULK_MAX_OPS);

 out:
	isi_error_handle(error, error_out);

	return total_cnt;
}

bool
isi_cfm_mapinfo_store_sbt::remove_at(off_t off, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	btree_key_t key = {{off, 0}};
	const void *ent_ptr = NULL;
	size_t ent_sz = 0;
	int res = 0;

	res = ifs_sbt_cond_remove_entry(sbt_fd_, &key, BT_CM_NONE, ent_ptr,
	    ent_sz, NULL);

	if (res != 0 && errno != ENOENT) {
		error = isi_system_error_new(errno,
		    "failed to remove sbt entry: %s",
		    cb_->get_store_name().c_str());
	}

	isi_error_handle(error, error_out);

	return res == 0;
}

bool
isi_cfm_mapinfo_store_sbt::remove_all(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	isi_cfm_mapentry entries[SBT_BULK_MAX_OPS];
	struct sbt_bulk_entry bulk_ops[SBT_BULK_MAX_OPS];

	int cnt = 0;
	int res = 0;

	do {
		bzero(bulk_ops, sizeof(bulk_ops));

		cnt = this->get(0, entries, SBT_BULK_MAX_OPS, &error);

		ON_ISI_ERROR_GOTO(out, error, "failed to get entry");

		if (cnt == 0)
			break;

		for (int i = 0; i < cnt; i++) {
			bulk_ops[i].fd = sbt_fd_;
			bulk_ops[i].op_type = SBT_SYS_OP_DEL;
			bulk_ops[i].key.keys[0] = entries[i].get_offset();
			bulk_ops[i].key.keys[1] = 0;
		}

		res = ifs_sbt_bulk_op(cnt, bulk_ops);

		if (res != 0) {
			error = isi_system_error_new(errno,
			    "failed to remove entries: %s",
			    cb_->get_store_name().c_str());
			goto out;
		}
	} while (cnt == SBT_BULK_MAX_OPS);

 out:
	isi_error_handle(error, error_out);

	return res == 0;
}

void
isi_cfm_mapinfo_store_sbt::hash_by_filename(const std::string &fname,
    char *folder1, char *folder2)
{
	// calculate the sub folders
	uint64_t hash = isi_hash64(fname.c_str(), fname.size(), 0);
	uint32_t l1 = *((uint32_t *) &hash);
	uint32_t l2 = *(((uint32_t *) &hash) + 1);

	sprintf(folder1, "%04d", l1 % HASH_LEVEL);
	sprintf(folder2, "%04d", l2 % HASH_LEVEL);
}

int
isi_cfm_mapinfo_store_sbt::open_folder(const std::string &fname,
    bool create, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int parent_fd = -1;
	char hashed_folder[2][8];
	const char *folder_chain[] =
	    {SBT_MAPINFO_DIR, hashed_folder[0], hashed_folder[1]};
	const int n = sizeof(folder_chain) / sizeof(folder_chain[0]);

	// calculate the sub folders
	hash_by_filename(fname, hashed_folder[0], hashed_folder[1]);

	// <SBT_ROOT>/cloudpools_mapinfo/hash_l1/hash_l2/fname
	parent_fd = sbt_open_root_dir(O_RDONLY);
	if (parent_fd < 0) {
		error = isi_system_error_new(errno,
		    "failed to open sbt root dir: %s", strerror(errno));
		goto out;
	}

	// open each sub folder
	for (int i = 0; i < n; ++i) {
		int sub_fd = enc_openat(
		    parent_fd, folder_chain[i], ENC_DEFAULT, O_RDONLY);

		// try to create the dir
		if (sub_fd < 0 && errno == ENOENT && create) {
			int res = enc_mkdirat(
			    parent_fd, folder_chain[i], ENC_DEFAULT, 0700);

			if (res == 0 || errno == EEXIST) {
				sub_fd = enc_openat(parent_fd,
				    folder_chain[i], ENC_DEFAULT, O_RDONLY);
			}
		}

		if (sub_fd < 0) {
			error = isi_system_error_new(errno,
			    "failed to open/create sbt folder: %s",
			    strerror(errno));
		}

		close(parent_fd);
		parent_fd = sub_fd;

		if (parent_fd < 0)
			break;
	}

 out:
	isi_error_handle(error, error_out);

	return parent_fd;
}

bool
isi_cfm_mapinfo_store_sbt::equals(const isi_cfm_mapinfo_store &to,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int cnt_this = SBT_BULK_MAX_OPS, cnt_that = SBT_BULK_MAX_OPS;
	isi_cfm_mapentry entries_this[SBT_BULK_MAX_OPS];
	isi_cfm_mapentry entries_that[SBT_BULK_MAX_OPS];
	off_t off = 0;

	do {
		cnt_this = this->get(off, entries_this, cnt_this, &error);

		ON_ISI_ERROR_GOTO(out, error, "failed to get sbt entry");

		cnt_that = to.get(off, entries_that, cnt_that, &error);

		ON_ISI_ERROR_GOTO(out, error, "failed to get sbt entry");

		if (cnt_this != cnt_that)
			return false;

		for (int i = 0; i < cnt_this; i++) {
			if (entries_this[i] != entries_that[i])
				return false;
		}

		off = entries_this[cnt_this - 1].get_offset() + 1;
	} while (cnt_this == SBT_BULK_MAX_OPS);

 out:
	isi_error_handle(error, error_out);

	return *error_out == NULL ? true : false;
}

bool
isi_cfm_mapinfo_store_sbt::exists(struct isi_error **error_out)
{
	int parent_fd = -1;
	struct stat st;
	int res = -1;
	sbt_fname_ = cb_->get_store_name();
	sbt_suffix_ = get_sbt_suffix(cb_);
	std::string sbt_fname_suffix = sbt_fname_ + sbt_suffix_;

	parent_fd = open_folder(sbt_fname_, false, error_out);
	if (parent_fd >= 0) {
		res = enc_fstatat(parent_fd,
		    sbt_fname_suffix.c_str(), ENC_DEFAULT, &st, 0);

		close(parent_fd);
	}

	return res == 0;
}

int
isi_cfm_mapinfo_store_sbt::tx_commit(struct tx_op ops[], int nops,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	struct btree_flags flags = {};
	std::vector<struct sbt_bulk_entry> sbt_ops(nops);

	int res = 0;
	int real_nops = 0;
	int entry_num = 0;

	ASSERT(nops > 0 && nops <= SBT_BULK_MAX_OPS);

	for (int i = 0; i < nops; ++i) {
		sbt_bulk_op_type op_type;

		if (ops[i].op == tx_op::DEL) {
			op_type = SBT_SYS_OP_DEL;
			--entry_num;
		}
		else if (ops[i].op == tx_op::ADD) {
			op_type = SBT_SYS_OP_ADD;
			++entry_num;
		}
		else if (ops[i].op == tx_op::MOD) {
			op_type = SBT_SYS_OP_MOD;
		}
		else
			continue;

		sbt_ops[real_nops].fd = sbt_fd_;
		sbt_ops[real_nops].op_type = op_type;
		sbt_ops[real_nops].key.keys[0] = ops[i].entry.get_offset();
		sbt_ops[real_nops].key.keys[1] = 0;

		if (op_type == SBT_SYS_OP_DEL) {
			sbt_ops[real_nops].entry_buf = NULL;
			sbt_ops[real_nops].entry_size = 0;
		}
		else {
			// true: not to pack in extended attributes but pack in sbt.
			ops[i].entry.pack(cb_->get_type(), true);
			sbt_ops[real_nops].entry_buf =
			    ops[i].entry.get_packed_entry();
			sbt_ops[real_nops].entry_size =
			    ops[i].entry.get_packed_size();
		}
		sbt_ops[real_nops].entry_flags = flags;
		sbt_ops[real_nops].cm = BT_CM_NONE;
		sbt_ops[real_nops].old_entry_buf = NULL;
		sbt_ops[real_nops].old_entry_size = 0;
		sbt_ops[real_nops].old_entry_flags = flags;

		++real_nops;
	}

	res = ifs_sbt_bulk_op(real_nops, sbt_ops.data());

	if (res != 0) {
		error = isi_system_error_new(errno,
		    "failed to commit sbt operation");
	}

	isi_error_handle(error, error_out);

	return entry_num;
}

void
isi_cfm_mapinfo_store_sbt::get_store_path(std::string &str)
{
	char hashed_folder[2][8];
	std::stringstream ss;
	std::string fname = cb_->get_store_name();

	hash_by_filename(fname, hashed_folder[0], hashed_folder[1]);
	ss << "/" << hashed_folder[0];
	ss << "/" << hashed_folder[1];
	ss << "/" << fname;

	str = ss.str();
}
