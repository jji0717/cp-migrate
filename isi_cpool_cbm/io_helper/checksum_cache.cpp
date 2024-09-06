
#include <isi_util_cpp/scoped_lock.h>
#include <isi_cpool_cbm/isi_cbm_scoped_ppi.h>
#include <isi_cpool_cbm/isi_cbm_policyinfo.h>
#include <isi_pools_fsa_gcfg/smartpools_util.h>

#include <sstream>
#include "checksum_cache.h"

namespace {
	const size_t CHECKSUM_CACHE_SIZE = 1024;
}

checksum_cache::checksum_cache() : checksum_cache_size(CHECKSUM_CACHE_SIZE)
{
	struct isi_error *error = NULL;
	scoped_ppi_reader ppi_rdr;
	const isi_cfm_policy_provider *cbm_cfg = ppi_rdr.get_ppi(&error);

	if (error)
		isi_error_free(error);
	else {
		checksum_cache_size =
		    cbm_cfg->cp_context->cfg->checksum_cache_size;
	}

}

bool checksum_cache::get(const std::string &container,
    const std::string &object_id, off_t off, checksum_value &value,
    sparse_area_header_t &spa)
{
	bool found = false;
	std::stringstream ss;
	checksum_key key;

	ss << container << object_id << "_" << off;
	key = ss.str();

	// protect the re-entry
	scoped_lock lock(*mtx);

	if (cache_map.find(key) != cache_map.end()) {
		found = true;
		// move to head if it is not the first one
		if (cache_map[key] != sorted_list.begin()) {
			sorted_list.push_front(*cache_map[key]);
			sorted_list.erase(cache_map[key]);
			cache_map[key] = sorted_list.begin();
		}

		value = cache_map[key]->value;
		spa   = cache_map[key]->spa;
	}
	return found;
}

uint32_t checksum_cache::invalidate(const std::string &container,
    const std::string &object_id, off_t off)
{
	uint32_t count = 0;
	std::stringstream ss;
	checksum_key key;

	ss << container << object_id << "_" << off;
	key = ss.str();

	// protect the re-entry
	scoped_lock lock(*mtx);

	if (cache_map.find(key) != cache_map.end()) {
		sorted_list.erase(cache_map[key]);
		count = cache_map.erase(key);
	}
	return count;
}

void checksum_cache::put(const std::string &container,
    const std::string &object_id, off_t off, checksum_value &value,
    sparse_area_header_t &spa)
{
	std::stringstream ss;
	checksum_key key;

	ss << container << object_id << "_" << off;
	key = ss.str();

	checksum item(key, value, spa);

	// protect the re-entry
	scoped_lock lock(*mtx);

	if (cache_map.find(key) != cache_map.end())
		sorted_list.erase(cache_map[key]);

	sorted_list.push_front(item);
	cache_map[key] = sorted_list.begin();

	// erase the oldest record
	if (cache_map.size() > checksum_cache_size) {
		std::string &key = sorted_list.rbegin()->key;

		if (cache_map.find(key) != cache_map.end())
			cache_map.erase(key);

		sorted_list.pop_back();
	}
}
