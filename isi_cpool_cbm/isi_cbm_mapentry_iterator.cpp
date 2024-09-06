
#include <isi_util_cpp/isi_exception.h>
#include "isi_cbm_error.h"
#include "isi_cbm_mapentry_iterator.h"

isi_cfm_mapentry_iterator &
isi_cfm_mapentry_iterator::operator=(
    const isi_cfm_mapentry_iterator &oth)
{
	offset_ = oth.offset_;
	in_store_ = oth.in_store_;
	in_memory_ = oth.in_memory_;
	entry_.reset(NULL);

	if (oth.entry_.get())
		entry_.reset(new pair_t(*oth.entry_.get()));

	return *this;
}

isi_cfm_mapentry_iterator &
isi_cfm_mapentry_iterator::next(struct isi_error **error_out)
{
	if (this->offset_ == END)
		return *this;

	if (in_store_) {
		isi_cfm_mapentry entry;
		off_t next_offset = offset_ + 1;

		in_store_->get(next_offset, entry,
		    isi_cfm_mapinfo_store::AT_OR_AFTER, error_out);

		if (*error_out) {
			this->offset_ = END;
			entry_.reset(NULL);
		}
		else {
			entry_.reset(new pair_t(entry.get_offset(), entry));
			this->offset_ = entry.get_offset();
		}
	}
	else if (in_memory_) {
		std::map<off_t, isi_cfm_mapentry>::iterator itr =
		    in_memory_->upper_bound(this->offset_);

		this->offset_ = (itr == in_memory_->end() ? END : itr->first);
	}

	return *this;
}

isi_cfm_mapentry_iterator &
isi_cfm_mapentry_iterator::prev(struct isi_error **error_out)
{
	if (in_store_ && offset_ != 0) {
		// --end() moves to last, otherwise move to previouse entry
		off_t prev_offset = offset_ > 0 ?
		    offset_ - 1 : std::numeric_limits<off_t>::max();
		isi_cfm_mapentry entry;

		in_store_->get(prev_offset, entry,
		    isi_cfm_mapinfo_store::AT_OR_BEFORE, error_out);

		// --begin() equals to begin(), no offset_ update here
		if (!*error_out) {
			entry_.reset(new pair_t(entry.get_offset(), entry));
			this->offset_ = entry.get_offset();
		}
	}
	else if (in_memory_) {
		// --end() moves to last, otherwise move to previous entry
		std::map<off_t, isi_cfm_mapentry>::iterator itr = offset_ < 0 ?
		    in_memory_->end() : in_memory_->find(this->offset_);

		// no attempt to move beyond begin()
		if (itr != in_memory_->begin()) {
			--itr;
			this->offset_ = itr->first;
		}
	}

	return *this;
}

isi_cfm_mapentry_iterator::pair_t &
isi_cfm_mapentry_iterator::operator *()
{
	if (in_store_) {
		ASSERT(entry_.get());

		return *entry_;
	}
	else if (in_memory_) {
		memory_t::iterator itr = in_memory_->find(offset_);

		ASSERT(itr != in_memory_->end());

		return *itr;
	}
	else
		ASSERT(false);
}

isi_cfm_mapentry_iterator::pair_t *
isi_cfm_mapentry_iterator::operator->()
{
	return &this->operator*();
}
