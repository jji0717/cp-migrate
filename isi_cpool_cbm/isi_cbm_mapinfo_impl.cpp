
#include <map>
#include <vector>

#include <isi_util_cpp/isi_exception.h>
#include <isi_ilog/ilog.h>
#include "isi_cbm_mapinfo_impl.h"
#include "isi_cbm_mapper.h"

isi_cfm_mapinfo_impl::isi_cfm_mapinfo_impl(isi_cfm_mapinfo *mapinfo)
    : factory_(new isi_cfm_mapinfo_store_sbt_factory()), mapinfo_(mapinfo)
{
}

void
isi_cfm_mapinfo_impl::open_store(bool create_if_not_exist,
    bool overwrite_if_exist, bool *created,
    struct isi_error **error_out)
{
	// initialize the store
	store_.reset(factory_->create(this));
	store_->open(create_if_not_exist, overwrite_if_exist, created,
	    error_out);
}

void
isi_cfm_mapinfo_impl::move_store_on_name_change(struct isi_error **error_out)
{
	if (!store_.get())
		return;

	// move store if store has a different name
	store_->move(error_out);
}

isi_cfm_mapinfo_impl::iterator
isi_cfm_mapinfo_impl::begin(struct isi_error **error_out)
{
	if (is_in_memory()) {
		return in_memory_.size() > 0?
		    iterator(&in_memory_, in_memory_.begin()->first) :
		    iterator(&in_memory_);
	}
	else {
		off_t offset = 0;
		isi_cfm_mapentry entry;

		if (store_->get(offset, entry, store_t::AT_OR_AFTER, error_out))
			return iterator(store_.get(), offset, entry);
		else
			return iterator(store_.get());
	}
}

isi_cfm_mapinfo_impl::iterator
isi_cfm_mapinfo_impl::begin(struct isi_error **error_out) const
{
	return const_cast<isi_cfm_mapinfo_impl *>(this)->begin(error_out);
}

isi_cfm_mapinfo_impl::iterator
isi_cfm_mapinfo_impl::end()
{
	return is_in_memory()?
	    iterator(&in_memory_) : iterator(store_.get());
}

isi_cfm_mapinfo_impl::iterator
isi_cfm_mapinfo_impl::end() const
{
	return const_cast<isi_cfm_mapinfo_impl *>(this)->end();
}

isi_cfm_mapinfo_impl::iterator
isi_cfm_mapinfo_impl::last(struct isi_error **error_out)
{
	return is_in_memory()?
	     iterator(&in_memory_).prev(error_out) :
	     iterator(store_.get()).prev(error_out);
}

isi_cfm_mapinfo_impl::iterator
isi_cfm_mapinfo_impl::last(struct isi_error **error_out) const
{
	return const_cast<isi_cfm_mapinfo_impl *>(this)->last(error_out);
}

isi_cfm_mapinfo_impl::iterator
isi_cfm_mapinfo_impl::find(off_t offset, struct isi_error **error_out)
{
	if (is_in_memory()) {
		memory_t::iterator itr = in_memory_.find(offset);

		return itr == in_memory_.end()?
		    iterator(&in_memory_) : iterator(&in_memory_, itr->first);
	}
	else {
		isi_cfm_mapentry entry;

		if (store_->get(offset, entry, store_t::AT, error_out))
			return iterator(store_.get(), offset, entry);
		else
			return iterator(store_.get());
	}
}

isi_cfm_mapinfo_impl::iterator
isi_cfm_mapinfo_impl::find(off_t offset, struct isi_error **error_out) const
{
	return const_cast<isi_cfm_mapinfo_impl *>(this)->find(offset, error_out);
}

isi_cfm_mapinfo_impl::iterator
isi_cfm_mapinfo_impl::lower_bound(off_t offset, struct isi_error **error_out)
{
	if (is_in_memory()) {
		memory_t::iterator itr = in_memory_.lower_bound(offset);

		return itr == in_memory_.end()?
		    iterator(&in_memory_) : iterator(&in_memory_, itr->first);
	}
	else {
		isi_cfm_mapentry entry;

		if (store_->get(offset, entry, store_t::AT_OR_AFTER, error_out))
			return iterator(store_.get(), offset, entry);
		else
			return iterator(store_.get());
	}
}

isi_cfm_mapinfo_impl::iterator
isi_cfm_mapinfo_impl::lower_bound(off_t offset,
    struct isi_error **error_out) const
{
	return const_cast<isi_cfm_mapinfo_impl *>(this)->lower_bound(offset,
	    error_out);
}

isi_cfm_mapinfo_impl::iterator
isi_cfm_mapinfo_impl::upper_bound(off_t offset, struct isi_error **error_out)
{
	if (is_in_memory()) {
		memory_t::iterator itr = in_memory_.upper_bound(offset);

		return itr == in_memory_.end()?
		    iterator(&in_memory_) : iterator(&in_memory_, itr->first);
	}
	else {
		isi_cfm_mapentry entry;
		offset += 1;

		if (store_->get(offset, entry, store_t::AT_OR_AFTER, error_out))
			return iterator(store_.get(), offset, entry);
		else
			return iterator(store_.get());
	}
}

isi_cfm_mapinfo_impl::iterator
isi_cfm_mapinfo_impl::upper_bound(off_t offset,
    struct isi_error **error_out) const
{
	return const_cast<isi_cfm_mapinfo_impl *>(this)->upper_bound(offset,
	    error_out);
}

void
isi_cfm_mapinfo_impl::erase(iterator &it, struct isi_error **error_out)
{
	if (is_in_memory()) {
		if (in_memory_.find(it->first) != in_memory_.end()) {
			in_memory_.erase(it->first);
			--mapinfo_->count;
		}
	}
	else {
		store_->remove(it->first, error_out);

		if (!*error_out)
			--mapinfo_->count;
	}
}

void
isi_cfm_mapinfo_impl::erase(off_t offset, struct isi_error **error_out)
{
	if (is_in_memory()) {
		if (in_memory_.find(offset) != in_memory_.end()) {
			in_memory_.erase(offset);
			--mapinfo_->count;
		}
	}
	else {
		store_->remove(offset, error_out);

		if (!*error_out)
			--mapinfo_->count;
	}
}

void
isi_cfm_mapinfo_impl::remove(struct isi_error **error_out)
{
	if (is_in_memory()) {
		in_memory_.clear();
		mapinfo_->count = 0;
	}
	else {
		ilog(IL_DEBUG, "delete store %s", get_store_name().c_str());
		// remove sbt file
		store_->remove(error_out);

		if (!*error_out) {
			store_.reset(NULL);
			mapinfo_->count = 0;
		}
	}
}

void
isi_cfm_mapinfo_impl::put(
    isi_cfm_mapentry &entry, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	off_t offset = entry.get_offset();

	if (is_in_memory()) {
		if (in_memory_.find(offset) == in_memory_.end())
			++mapinfo_->count;

		in_memory_[offset] = entry;
	}
	else {
		store_->put(offset, entry, &error);
		ON_ISI_ERROR_GOTO(out, error,
		    "failed to add mapentry at offset: %ld", offset);

		++mapinfo_->count;
	}

	// migrate to store if in-memory entries are over the threshold
	if (is_in_memory() &&
	    in_memory_.size() > (size_t)get_overflow_threshold()) {
		perform_overflow(&error);
		ON_ISI_ERROR_GOTO(out, error);
	}
 out:
	isi_error_handle(error, error_out);
}

int
isi_cfm_mapinfo_impl::copy_from(const isi_cfm_mapinfo_impl &src,
    struct isi_error **error_out)
{
	in_memory_.clear();

	if (src.is_in_memory()) {
		in_memory_ = src.in_memory_;
		mapinfo_->count = src.mapinfo_->count;
		return mapinfo_->count;
	}

	int local_count = 0;

	store_.reset(factory_->create(this));

	// skip the copy when they are the same store
	if (src.get_store_name() == get_store_name() &&
	    src.get_store_suffix() == get_store_suffix()) {
		local_count = src.mapinfo_->count;
		// no deep copy in this case.
		store_->open(false, false, NULL, error_out);
		ON_ISI_ERROR_GOTO(out, *error_out);
	}
	else {
		// if we are truly copying from one to another different store,
		// overwrite the existing if any
		store_->open(true, true, NULL, error_out);
		ON_ISI_ERROR_GOTO(out, *error_out);

		ilog(IL_DEBUG, "copy store %s -> %s", src.get_store_name().c_str(),
		    get_store_name().c_str());

		local_count = store_->copy_from(*src.store_, error_out);
	}

 out:
	return local_count;
}

bool
isi_cfm_mapinfo_impl::equals(const isi_cfm_mapinfo_impl &that,
    struct isi_error **error_out) const
{
	if (that.is_in_memory() != is_in_memory())
		return false;

	return is_in_memory() ?
	    in_memory_ == that.in_memory_ :
	    store_->equals(*that.store_, error_out);
}

bool
isi_cfm_mapinfo_impl::tx_commit(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	std::vector<struct tx_op>::iterator it;

	size_t entries_added = 0;

	if (tx_ops.size() == 0)
		return true;

	// consolidate DEL and ADD into MOD
	for (it = tx_ops.begin(); it != tx_ops.end(); ++it) {
		std::vector<struct tx_op>::iterator eit = it;

		if (it->op == tx_op::ADD)
			++entries_added;

		if (it->op != tx_op::DEL)
			continue;

		while (++eit != tx_ops.end()) {
			if (it->entry.get_offset() ==
			    eit->entry.get_offset()) {
				it->op = tx_op::INVALID;
				eit->op = tx_op::MOD;
				break;
			}
		}
	}

	if (is_in_memory() &&
	    in_memory_.size() + entries_added > get_overflow_threshold()) {
		perform_overflow(&error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	if (is_in_memory()) {
		for (it = tx_ops.begin(); it != tx_ops.end(); ++it) {
			if (it->op == tx_op::ADD || it->op == tx_op::MOD) {
				put(it->entry, &error);
				ON_ISI_ERROR_GOTO(out, error);
			}
			else if (it->op == tx_op::DEL) {
				erase(it->entry.get_offset(), &error);
				ON_ISI_ERROR_GOTO(out, error);
			}
		}
	}
	else {
		mapinfo_->count += store_->tx_commit(
		    tx_ops.data(), (int)tx_ops.size(), &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

out:
	// remove the records
	tx_ops.clear();

	isi_error_handle(error, error_out);
	return *error_out == NULL;
}

const isi_cloud_object_id &
isi_cfm_mapinfo_impl::get_object_id() const
{
	return mapinfo_->get_object_id();
}

int
isi_cfm_mapinfo_impl::get_overflow_threshold() const
{
	return mapinfo_->get_overflow_threshold();
}

int
isi_cfm_mapinfo_impl::get_mapentry_pack_size(bool pack_overflow) const
{
	return mapinfo_->get_mapentry_pack_size(pack_overflow);
}

int
isi_cfm_mapinfo_impl::get_type() const
{
	return mapinfo_->get_type();
}

std::string
isi_cfm_mapinfo_impl::get_store_name() const
{
	struct fmt FMT_INIT_CLEAN(str);
	const isi_cloud_object_id &oid = get_object_id();

	fmt_print(&str, "%s_%lu",
	    oid.get_cmo_name().c_str(), oid.get_snapid());

	return fmt_string(&str);
}

std::string
isi_cfm_mapinfo_impl::get_store_suffix() const
{
	std::string suffix;

	suffix = (get_type() & ISI_CFM_MAP_TYPE_WIP) ? ".wip" :
	    mapinfo_->get_store_suffix();

	return suffix;
}

void
isi_cfm_mapinfo_impl::perform_overflow(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (!is_in_memory())
		return;

	std::vector<isi_cfm_mapentry> mapentries(in_memory_.size());
	memory_t::iterator itr;

	ilog(IL_DEBUG, "overflow map entries to store %s",
	    get_store_name().c_str());

	this->open_store(true, false, NULL, &error);
	ON_ISI_ERROR_GOTO(out, error, "failed to open mapentry store");

	// remove all the entries in case the SBT exists
	// like unit test crashed
	store_->remove(-1, &error);
	ON_ISI_ERROR_GOTO(out, error,
	    "failed to cleanup mapentry store");

	// copy over
	itr = in_memory_.begin();

	for (int i = 0; itr != in_memory_.end(); ++itr, ++i)
		mapentries[i] = itr->second;

	// bulk insertion
	store_->put(mapentries.data(), in_memory_.size(), &error);

	ON_ISI_ERROR_GOTO(out, error);

	in_memory_.clear();

out:
	isi_error_handle(error, error_out);
}
