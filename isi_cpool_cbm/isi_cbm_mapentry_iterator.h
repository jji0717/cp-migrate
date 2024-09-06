
#pragma once

#include <map>
#include "isi_cbm_mapinfo_store.h"
#include "isi_cbm_mapentry.h"

class isi_cfm_mapentry_iterator
{
	typedef std::pair<const off_t, isi_cfm_mapentry> pair_t;

	typedef isi_cfm_mapinfo_store store_t;
	typedef std::map<off_t, isi_cfm_mapentry> memory_t;

	enum { END = -1 };

 public:
	/**
	 * constructs the default end() iterator
	 */
	isi_cfm_mapentry_iterator()
	    : in_store_(NULL), in_memory_(NULL), offset_(END)
	{
	}

	/**
	 * constructs end() iterator to the external store
	 */
	isi_cfm_mapentry_iterator(store_t *store, off_t offset = END)
	    : in_store_(store), in_memory_(NULL), offset_(offset)
	{
		ASSERT(in_store_);
	}

	/**
	 * constructs iterator points to the external store
	 */
	isi_cfm_mapentry_iterator(store_t *store, off_t offset,
	    isi_cfm_mapentry &entry)
	    : in_store_(store), in_memory_(NULL), offset_(offset),
	    entry_(new pair_t(offset, entry))
	{
		ASSERT(in_store_);
	}

	/**
	 * constructs end() iterator to the memory map
	 */
	isi_cfm_mapentry_iterator(memory_t *memory, off_t offset = END)
	    : in_store_(NULL), in_memory_(memory), offset_(offset)
	{
		ASSERT(in_memory_);
	}

	/**
	 * copy constructor
	 */
	isi_cfm_mapentry_iterator(const isi_cfm_mapentry_iterator &oth)
            : in_store_(oth.in_store_), in_memory_(oth.in_memory_),
	    offset_(oth.offset_)
	{
		// copy the entry if any, so it is not going to load from store
		if (in_store_ && oth.entry_.get())
			entry_.reset(new pair_t(*oth.entry_.get()));
	}

	/**
	 * assigns from another iterator
	 */
	isi_cfm_mapentry_iterator &operator=(
	    const isi_cfm_mapentry_iterator &oth);

	/**
	 * compares two iterators
	 */
	bool operator==(const isi_cfm_mapentry_iterator &oth)
	{
		return oth.offset_ == this->offset_;
	}

	/**
	 * compares two iterators
	 */
	bool operator!=(const isi_cfm_mapentry_iterator &oth)
	{
		return oth.offset_ != this->offset_;
	}

	/**
	 * moves to the following adjacent entry
	 */
	isi_cfm_mapentry_iterator &next(struct isi_error **error_out);

	/**
	 * moves to the prviouse adjacent entry
	 */
	isi_cfm_mapentry_iterator &prev(struct isi_error **error_out);

	/**
	 * get the reference of the entry
	 */
	pair_t &operator *();

	/**
	 * get the address of the entry
	 */
	pair_t *operator->();


 private:
	store_t *in_store_;

	memory_t *in_memory_;

	off_t offset_;

	std::auto_ptr<pair_t> entry_;
};

