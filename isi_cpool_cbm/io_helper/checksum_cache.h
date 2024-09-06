#ifndef __CHECKSUM_CACHE_H__
#define __CHECKSUM_CACHE_H__

#include <map>
#include <list>
#include <string>
#include "checksum_header.h"
#include "sparse_area_header.h"

typedef std::string checksum_key;
typedef checksum_header checksum_value;

// checksum value
struct checksum {
	checksum(const checksum_key &key, const checksum_value &value,
	    sparse_area_header_t &spa)
	    : key(key), value(value), spa(spa)
	{ }

	checksum(const checksum &o) :
	    key(o.key), value(o.value), spa(o.spa) {}

	checksum &operator=(const checksum &o) {
		this->key = o.key;
		this->value = o.value;
		this->spa = o.spa;
		return *this;
	}

	checksum_key key;
	checksum_value value;
	sparse_area_header_t spa;

};

typedef std::list<checksum>::iterator checksum_ref;


class checksum_cache
{
public:
	/**
	 * constructor that loads the default checksum cache size from
	 * the configuration, failure throws isi_exception
	 */
	checksum_cache();

	/**
	 * get the checksum and sparse area header by container, object
	 * id, and offset Returns a boolean: entry found true, else
	 * false
	 */
	bool get(const std::string &container,
	    const std::string &object_id, off_t off,
	    checksum_value &value, sparse_area_header_t &spa);

	/**
	 * invalidate a single entry selected by
	 * container, object id, and offset
	 * Returns the count of elements removed (0 or 1)
	 */
	uint32_t invalidate(const std::string &container,
	    const std::string &object_id, off_t off);

	/**
	 * cache up the checksum indexed by
	 * container, object id, and offset
	 */
	void put(const std::string &container, const std::string &object_id,
	    off_t off, checksum_value &value, sparse_area_header_t &spa);

	/**
	 * cleanup the cache map, define for unit test
	 */
	void cleanup() {
		cache_map.clear();
		sorted_list.clear();
	}

private:
	scoped_mutex mtx;

	size_t checksum_cache_size;

	std::map<checksum_key, checksum_ref> cache_map;

	std::list<checksum> sorted_list;
};

#endif
