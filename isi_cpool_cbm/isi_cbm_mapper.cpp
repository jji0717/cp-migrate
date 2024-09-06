#include <cstdio>
#include <errno.h>
#include <string>
#include <sys/stat.h>
#include <sys/extattr.h>
#include <ifs/ifs_types.h>
#include <ifs/ifs_lin_open.h>
#include <isi_ilog/ilog.h>
#include <iostream> // XXXegc: debug
#include <isi_cloud_common/isi_cpool_version.h>
#include "isi_cloud_api/isi_cloud_api.h"
#include "isi_cpool_cbm/isi_cbm_mapper.h"
#include "isi_cpool_cbm/isi_cbm_write.h"
#include "isi_cpool_cbm/isi_cbm_util.h"
#include <isi_cpool_d_common/isi_cloud_object_id.h>
#include "isi_cbm_error.h"
#include "isi_cbm_account_util.h"

using namespace isi_cloud;

static inline void
packit(void **dst, const void *src, int sz)
{
	if (sz > 0)
	{
		memcpy(*dst, src, sz);
		*(char **)dst += sz;
	}
}

static inline void
unpackit(void **src, void *dst, int sz)
{
	if (sz > 0)
	{
		memcpy(dst, *src, sz);
		*(char **)src += sz;
	}
}

void format_bucket_name(struct fmt &bucket_fmt, const isi_cbm_id &acct_id)
{
	fmt_print(&bucket_fmt, "d%si%u",
			  acct_id.get_cluster_id(), acct_id.get_id());
}

/*
 * short cuts for getting offset and length from map iterators
 */
#define GETOFF(a) ((a)->second.get_offset())
#define GETLEN(a) ((a)->second.get_length())
#define GETACC(a) ((a)->second.get_account_id())
#define GETCON(a) ((a)->second.get_container())
#define GETOBJ(a) ((a)->second.get_object_id())

void isi_cfm_mapinfo::set_version(int v)
{
	version = v;
}

bool isi_cfm_mapinfo::equals(const isi_cfm_mapinfo &that,
							 struct isi_error **error_out) const
{
	bool result = true;
	struct isi_error *error = NULL;

	void *that_packed_map = NULL;
	int that_packed_size = 0;

	void *this_packed_map = NULL;
	int this_packed_size = 0;

	that.pack(that_packed_size, &that_packed_map, &error);
	ON_ISI_ERROR_GOTO(out, error, "Failed to pack mapinfo");

	pack(this_packed_size, &this_packed_map, &error);
	ON_ISI_ERROR_GOTO(out, error, "Failed to pack mapinfo");

	if (this_packed_size != that_packed_size ||
		memcmp(this_packed_map, that_packed_map, this_packed_size))
		result = false;

	result &= impl.equals(that.impl, &error);
	ON_ISI_ERROR_GOTO(out, error, "Failed to compare the mapinfo");

out:
	if (that_packed_map)
		free(that_packed_map);

	if (this_packed_map)
		free(this_packed_map);

	isi_error_handle(error, error_out);

	return result;
}

void isi_cfm_mapinfo::copy(const isi_cfm_mapinfo &that, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	void *that_packed_map = NULL;
	int that_packed_size = 0;

	isi_cloud_object_id empty_id;
	isi_cloud_object_id id = get_object_id(); // save the id
	int map_type = get_type();
	clear();

	// remove entries only when source and target are not
	// referencing the same store
	if (!that.is_overflow() ||
		impl.get_store_name() != that.impl.get_store_name())
	{
		remove_store(&error);

		ON_ISI_ERROR_GOTO(out, error,
						  "failed to cleanup before store copy");
	}

	that.pack(that_packed_size, &that_packed_map, &error);
	ON_ISI_ERROR_GOTO(out, error, "failed to pack the mapinfo");

	unpack(that_packed_map, that_packed_size, &error);
	ON_ISI_ERROR_GOTO(out, error, "failed to unpack the mapinfo");

	if (id != empty_id)
	{
		set_object_id(id); // if the id was set, keep it
	}

	if (map_type & ISI_CFM_MAP_TYPE_WIP)
	{ // if wip was set, keep it
		set_type(get_type() | ISI_CFM_MAP_TYPE_WIP);
	}

	if (is_overflow())
	{
		// copy when store names are different
		// otherwise they share the same store
		impl.copy_from(that.impl, &error);

		ON_ISI_ERROR_GOTO(out, error, "failed to copy mapentry store");
	}

out:
	if (that_packed_map)
		free(that_packed_map);

	if (!error)
		ASSERT(that.get_count() == get_count());

	isi_error_handle(error, error_out);
}

/**
 * Get the iterator to the map whose key is the largest but <=
 * to the given offset. If not found, return false.
 */
bool isi_cfm_mapinfo::get_map_iterator_for_offset(map_iterator &it,
												  off_t offset, struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	bool not_begin = false;

	it = impl.lower_bound(offset, &error);
	ON_ISI_ERROR_GOTO(out, error, "Failed to find the entry by offset");

	if (it != impl.end() && GETOFF(it) == offset)
		return true;

	not_begin = (it != impl.begin(&error));
	ON_ISI_ERROR_GOTO(out, error, "Failed to start the mapentry iterator");

	if (not_begin)
	{
		it.prev(&error);
		ON_ISI_ERROR_GOTO(out, error, "Failed to move to previous entry");
	}

out:
	isi_error_handle(error, error_out);

	return it != impl.end();
}

/**
 * Get the iterator to the map entry that contains the offset
 * given. If the offset is not contained by any map entry it
 * will return false and the iterator will point to
 * impl.end()
 */
bool isi_cfm_mapinfo::get_containing_map_iterator_for_offset(map_iterator &it,
															 off_t offset, struct isi_error **error_out) const
{
	bool not_begin = false;

	it = impl.lower_bound(offset, error_out);
	ON_ISI_ERROR_GOTO(out, *error_out, "Failed to find the entry by offset");

	if (it != impl.end() && GETOFF(it) == offset)
		return true;

	not_begin = (it != impl.begin(error_out));
	ON_ISI_ERROR_GOTO(out, *error_out,
					  "Failed to start the mapentry iterator");

	if (not_begin)
	{
		it.prev(error_out);
		ON_ISI_ERROR_GOTO(out, *error_out, "Failed to move to previous");
	}

	if (it == impl.end())
		return false;

	if (offset >= GETOFF(it) && offset < (off_t)(GETLEN(it) + GETOFF(it)))
		return true;
	it = impl.end();

out:
	return false;
}

static inline void
set_end(isi_cfm_mapentry &entry, size_t end)
{
	entry.set_length(end - entry.get_offset());
}

/*
 * This routine will update the map for a stub by adding new entries to the
 * map.  The routine will handle the case where the new record contains an
 * overlap with one or more existing records and updates the records
 * accordingly.  It is assumed that the overlaps will be on object boundaries
 * to that objects themselves are not being modified by these actions.  Any
 * removed operations will be added to a list of depricated objects inside the
 * map itself for reference elsewhere.
 * [XXXEGC: removal TBD, not needed for the current work.]
 */
bool isi_cfm_mapinfo::add(isi_cfm_mapentry &newentry, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	size_t newentry_end = newentry.get_offset() + newentry.get_length();

	iterator p = impl.lower_bound(newentry_end, &error);

	/*
	 * Don't add packed information to the entry being added,
	 * just the entry itself, free up any packed data.
	 */
	newentry.free_packed_entry();

	// coalesce with the following entry if needed
	if (p != impl.end() && GETOFF(p) == newentry_end && data_match(newentry, p->second))
	{
		newentry_end = GETOFF(p) + GETLEN(p);
		set_end(newentry, newentry_end);

		impl.tx_erase(p->first);
	}

	// iterate back through all the overlapping entries
	// and remove/merge/split them as needed
	while (true)
	{
		if (p == impl.begin(&error) || error)
		{
			ON_ISI_ERROR_GOTO(out, error);
			break;
		}

		p.prev(&error);
		ON_ISI_ERROR_GOTO(out, error);

		ASSERT(GETOFF(p) < newentry_end);

		size_t p_end = GETOFF(p) + GETLEN(p);

		if (p_end <= newentry.get_offset())
			break;

		/* remove old entry */
		impl.tx_erase(p->first);

		if (p_end > newentry_end)
		{
			if (data_match(newentry, p->second))
			{
				newentry_end = p_end;
				set_end(newentry, newentry_end);
			}
			else
			{
				isi_cfm_mapentry fragment(p->second);

				fragment.set_offset(newentry_end);
				set_end(fragment, p_end);

				impl.tx_put(fragment);
			}
		}

		if (GETOFF(p) < newentry.get_offset())
		{
			if (data_match(newentry, p->second))
			{
				newentry.set_offset(GETOFF(p));
				set_end(newentry, newentry_end);
			}
			else
			{
				isi_cfm_mapentry fragment(p->second);

				fragment.set_offset(GETOFF(p));
				set_end(fragment, newentry.get_offset());

				impl.tx_put(fragment);
			}
		}
	}

	// coalesce with the previous entry if needed
	if (p != impl.end() && GETOFF(p) + GETLEN(p) == newentry.get_offset() && data_match(newentry, p->second))
	{
		newentry.set_offset(GETOFF(p));
		set_end(newentry, newentry_end);

		impl.tx_erase(p->first);
	}

	impl.tx_put(newentry);

	impl.tx_commit(&error);
	ON_ISI_ERROR_GOTO(out, error, "error adding entry <%ld,%ld>",
					  newentry.get_offset(), newentry.get_length());

	// always set the right status
	set_overflow(!impl.is_in_memory());

out:
	isi_error_handle(error, error_out);

	return *error_out == NULL;
}

void isi_cfm_mapinfo::remove(const isi_cfm_mapentry &mapentry,
							 struct isi_error **error_out)
{
	impl.erase(mapentry.get_offset(), error_out);
}

/**
 * Compare the data portions of two records, i.e. the fields other than the
 * offset and length
 */
bool isi_cfm_mapinfo::data_match(isi_cfm_mapentry &m1, isi_cfm_mapentry &m2)
{
	bool hascheckinfo = (type & ISI_CFM_MAP_TYPE_CHECKINFO);
	bool hascontainer = (type & ISI_CFM_MAP_TYPE_CONTAINER);
	bool hasobject = (type & ISI_CFM_MAP_TYPE_OBJECT);

	if (m1.get_index() != m2.get_index())
		return false;

	if (!m1.get_account_id().equals(m2.get_account_id()))
		return false;

	if (hasobject)
		if (m1.get_object_id() != m2.get_object_id())
			return false;
	if (hascontainer)
		if (m1.get_container() != m2.get_container())
			return false;

	if (hascheckinfo)
		if (!COMPARE_GUID(m1.get_checkinfo(), m2.get_checkinfo()))
			return false;
	return true;
}

#define DUMPCHECKINFO(a) (a).ig_time << "." << (a).ig_random

/*
 * Simple dump routine for a stub map to put the information out to stdout in
 * a human readable format.  Mainly for debugging purposes
 */
void isi_cfm_mapinfo::dump(bool dump_map, bool dump_paths) const
{
	iterator it;
	isi_cbm_ioh_base_sptr io_helper;
	struct isi_error *error = NULL;

	bool hascheckinfo = (type & ISI_CFM_MAP_TYPE_CHECKINFO);
	bool hascontainer = (type & ISI_CFM_MAP_TYPE_CONTAINER);
	bool hasobject = (type & ISI_CFM_MAP_TYPE_OBJECT);
	bool hasmdocheck = (type & ISI_CFM_MAP_TYPE_MAPCHECK);
	bool hascontainer_cmo = (type & ISI_CFM_MAP_TYPE_CONTAINER_CMO);
	bool compressed = (type & ISI_CFM_MAP_TYPE_COMPRESSED) != 0;
	bool encrypted = (type & ISI_CFM_MAP_TYPE_ENCRYPTED) != 0;

	if (!(dump_map || dump_paths))
	{
		return;
	}

	std::cout << "Version: " << version << std::endl;
	std::cout << "Header size: " << header_size << std::endl;
	std::cout << "Map type: " << type << std::endl;
	std::cout << "Number of map entries: " << count << std::endl;
	std::cout << "Chunksize: " << chunksize << std::endl;
	std::cout << "File size: " << filesize << std::endl;
	std::cout << "Read size: " << readsize << std::endl;
	std::cout << "Sparse resolution: " << sparse_resolution << std::endl;
	std::cout << "Policy ID: " << policy_id.get_id() << std::endl;
	std::cout << "Provider ID: " << provider_id.get_id() << std::endl;

	if (dump_map)
	{
		std::cout << "Compressed: " << compressed << std::endl;
		std::cout << "Encrypted: " << encrypted << std::endl;
	}

	if (dump_paths && (hascontainer_cmo || hasobject))
	{
		io_helper = isi_cpool_get_io_helper(
			account,
			&error);
		if (error || !io_helper)
		{
			std::cout << "*ERROR locating IO helper"
					  << std::endl;
		}
		if (error)
		{
			isi_error_free(error);
			error = NULL;
		}
	}
	if (dump_map && hascontainer_cmo)
	{
		std::cout << "CMO Container: " << container_cmo << std::endl;
		std::cout << "CMO Account: "
				  << account.get_id()
				  << " "
				  << account.get_cluster_id()
				  << std::endl;
		std::cout << "CMO Object: " << object_id_.to_string()
				  << std::endl;
		std::cout << "CMO SnapId: "
				  << object_id_.get_snapid()
				  << std::endl;
	}

	if (dump_paths && hascontainer_cmo)
	{
		std::string container_path("*ERROR locating IO helper");
		std::string cmo_name = io_helper->get_cmo_name(object_id_);
		if (io_helper)
		{
			io_helper->build_container_path(
				container_cmo, cmo_name,
				container_path);
		}
		std::cout << "CMO Path: "
				  << container_path
				  << "/"
				  << cmo_name
				  << std::endl;
	}

	if (dump_map)
	{
		std::cout << "MDO checksum: ";
		if (hasmdocheck)
			std::cout << DUMPCHECKINFO(checkinfo) << std::endl;
		else
			std::cout << "Not specified" << std::endl;
	}

	std::cout << "Overflowed: ";
	std::cout << (is_overflow() ? "Yes" : "No") << std::endl;
	if (is_overflow())
	{
		std::string store_path;

		impl.get_store_path(store_path);
		std::cout << "Store path: " << store_path << std::endl;
	}

	std::cout << "Mapping Entries Below: " << std::endl;
	for (it = impl.begin(&error); it != impl.end(); it.next(&error))
	{
		std::cout << "account(id): "
				  << it->second.get_account_id().get_id() << std::endl;

		std::cout << "offset: " << GETOFF(it) << std::endl;
		std::cout << "len: " << GETLEN(it) << std::endl;

		if (dump_map)
		{
			std::cout << "physical length: "
					  << it->second.get_plength() << std::endl;
		}
		if (hascontainer)
			std::cout << "bucket: " << it->second.get_container()
					  << std::endl;
		if (hasobject)
		{
			std::cout << "object: "
					  << it->second.get_object_id().to_string()
					  << std::endl;
			std::cout << "index: "
					  << it->second.get_index() << std::endl;
		}
		if (dump_map && hasobject)
		{
			std::cout << "snapid: "
					  << it->second.get_object_id().get_snapid()
					  << std::endl;
		}
		if (dump_paths && hasobject)
		{
			size_t len;
			int cdo_idx;
			std::string container("");
			std::string container_path("*ERROR locating IO helper");
			if (hascontainer)
			{
				container = it->second.get_container();
			}

			cdo_idx = (GETOFF(it) / chunksize) + 1;
			for (len = 0; len < GETLEN(it); len += chunksize)
			{
				std::string cdo_name =
					io_helper->get_cdo_name(
						it->second.get_object_id(),
						cdo_idx);
				if (io_helper)
				{
					io_helper->build_container_path(
						container,
						cdo_name,
						container_path);
				}
				std::cout << "CDO Path "
						  << cdo_idx
						  << ": "
						  << container_path
						  << "/"
						  << cdo_name
						  << std::endl;
				cdo_idx++;
			}
		}
		if (dump_map && hascheckinfo)
		{
			struct fmt FMT_INIT_CLEAN(fmtout);
			ifs_guid tmp = it->second.get_checkinfo();
			fmt_print(&fmtout, "%{}", ifs_guid_fmt(&tmp));
			std::cout << "checkinfo: " << fmt_string(&fmtout)
					  << std::endl;
		}
	}
	ON_ISI_ERROR_GOTO(out, error, "Failed to iterate the mapinfo");

out:
	if (error)
	{
		ilog(IL_ERR, "Failed to dump mapinfo: %{}", isi_error_fmt(error));
		isi_error_free(error);
	}
}

/*
 * Simple dump routine for a stub map to put the information out to syslog in
 * a human readable format.  Mainly for debugging purposes
 */
void isi_cfm_mapinfo::dump_to_ilog(bool dump_paths) const
{
	iterator it;
	int entry_index = 0;
	isi_cbm_ioh_base_sptr io_helper;
	struct isi_error *error = NULL;

	bool hascheckinfo = (type & ISI_CFM_MAP_TYPE_CHECKINFO);
	bool hascontainer = (type & ISI_CFM_MAP_TYPE_CONTAINER);
	bool hasobject = (type & ISI_CFM_MAP_TYPE_OBJECT);
	bool hasmdocheck = (type & ISI_CFM_MAP_TYPE_MAPCHECK);
	bool hascontainer_cmo = (type & ISI_CFM_MAP_TYPE_CONTAINER_CMO);
	bool compressed = (type & ISI_CFM_MAP_TYPE_COMPRESSED) != 0;
	bool encrypted = (type & ISI_CFM_MAP_TYPE_ENCRYPTED) != 0;

	ilog(IL_DEBUG, "Dump of mapinfo %p:: "
				   "Version: %d, "
				   "Map type: %d, "
				   "Number of map entries: %d, "
				   "Chunksize: %lld",
		 this,
		 version,
		 type,
		 count,
		 chunksize);

	ilog(IL_DEBUG, "Dump of mapinfo %p:: "
				   "File size: %lld, "
				   "Read size: %lld, "
				   "Sparse area size: %u, "
				   "Policy ID: %d, "
				   "Provider ID: %d",
		 this,
		 filesize,
		 readsize,
		 sparse_resolution,
		 policy_id.get_id(),
		 provider_id.get_id());

	ilog(IL_DEBUG, "Dump of mapinfo %p:: "
				   "Compressed: %d, "
				   "Encrypted: %d, ",
		 this,
		 compressed,
		 encrypted);

	if (dump_paths && (hascontainer_cmo || hasobject))
	{
		io_helper = isi_cpool_get_io_helper(
			account,
			&error);
		if (error || !io_helper)
		{
			ilog(IL_DEBUG, "***ERROR locating IO helper");
		}
		if (error)
		{
			isi_error_free(error);
			error = NULL;
		}
	}

	if (hascontainer_cmo)
	{
		ilog(IL_DEBUG, "Dump of mapinfo %p:: "
					   "CMO Container: %s, "
					   "CMO Account: %d, "
					   "Cluster id: %s, "
					   "CMO Object: %s, "
					   "CMO SnapId: %lld",
			 this,
			 container_cmo.c_str(),
			 account.get_id(),
			 account.get_cluster_id(),
			 object_id_.to_c_string(),
			 object_id_.get_snapid());
	}

	if (dump_paths && hascontainer_cmo)
	{
		std::string container_path("*ERROR*");
		std::string cmo_name = object_id_.get_cmo_name();
		if (io_helper)
		{
			io_helper->build_container_path(
				container_cmo, cmo_name,
				container_path);
		}
		ilog(IL_DEBUG, "CMO Path: %s/%s",
			 container_path.c_str(),
			 cmo_name.c_str());
	}

	if (hasmdocheck)
		ilog(IL_DEBUG, "Dump of mapinfo %p:: "
					   "MDO checksum: %lld.%lld",
			 this,
			 checkinfo.ig_time,
			 checkinfo.ig_random);
	else
		ilog(IL_DEBUG, "Dump of mapinfo %p:: "
					   "MDO checksum: Not specified",
			 this);

	for (it = impl.begin(&error); it != impl.end(); it.next(&error))
	{
		ilog(IL_DEBUG, "Dump of mapinfo %p, Entry %d:: "
					   "account(id): %d, "
					   "offset/len: %lld/%lld, "
					   "physical length: %lld ",
			 this, entry_index,
			 it->second.get_account_id().get_id(),
			 GETOFF(it),
			 GETLEN(it),
			 it->second.get_plength());

		ilog(IL_DEBUG, "Dump of mapinfo %p, Entry %d:: "
					   "bucket: %s, "
					   "object: %s, "
					   "index: %d, "
					   "snapid: %lld ",
			 this, entry_index,
			 (hascontainer) ? it->second.get_container().c_str() : "",
			 (hasobject) ? it->second.get_object_id().to_c_string() : "",
			 (hasobject) ? it->second.get_index() : 0,
			 (hasobject) ? it->second.get_object_id().get_snapid() : 0);

		if (dump_paths && hasobject)
		{
			size_t len;
			int cdo_idx;
			std::string container("");
			std::string container_path("*ERROR*");
			if (hascontainer)
			{
				container = it->second.get_container();
			}

			cdo_idx = (GETOFF(it) % chunksize) + 1;
			for (len = 0; len < GETLEN(it); len += chunksize)
			{
				std::string cdo_name =
					it->second.get_object_id().get_cdo_name(
						cdo_idx);
				if (io_helper)
				{
					io_helper->build_container_path(
						container,
						cdo_name,
						container_path);
				}
				ilog(IL_DEBUG, "CDO Path %d: %s/%s",
					 cdo_idx,
					 container_path.c_str(),
					 cdo_name.c_str());
				cdo_idx++;
			}
		}
		if (hascheckinfo)
		{
			ifs_guid tmp = it->second.get_checkinfo();
			ilog(IL_DEBUG, "Dump of mapinfo %p, Entry %d:: "
						   "cacheinfo: %{} ",
				 this, entry_index, ifs_guid_fmt(&tmp));
		}
		entry_index++;
	}
	ON_ISI_ERROR_GOTO(out, error, "Failed to iterate the mapinfo");

out:
	if (error)
	{
		ilog(IL_ERR, "Failed to dump mapinfo: %{}", isi_error_fmt(error));
		isi_error_free(error);
	}
}

#define CHECK_OFFLEN(a, b)                     \
	(((a).get_offset() == (b).get_offset()) && \
	 ((a).get_length() == (b).get_length()))

/*
 * simple routine utilized for map unit testing to compare the current map to
 * an array of entries passed in.
 */
bool isi_cfm_mapinfo::verify(struct verifyer *v, int v_size)
{
	iterator it;
	struct isi_error *error = NULL;
	isi_cfm_mapentry entry;
	int i;

	if (v_size != count)
		return false;

	for (i = 0, it = impl.begin(&error); it != impl.end();
		 ++i, it.next(&error))
	{
		struct fmt FMT_INIT_CLEAN(bucket_fmt);

		format_bucket_name(bucket_fmt, v[i].account_id);

		entry.setentry(CPOOL_CFM_MAP_VERSION, v[i].account_id, v[i].offset, v[i].length,
					   fmt_string(&bucket_fmt), v[i].object);

		if (!(data_match(it->second, entry) &&
			  CHECK_OFFLEN(it->second, entry)))
			return false;
	}
	ON_ISI_ERROR_GOTO(out, error, "Failed to iterate the mapinfo");

out:
	if (error)
	{
		ilog(IL_ERR, "Failed to dump mapinfo: %{}", isi_error_fmt(error));
		isi_error_free(error);
	}

	return true;
}

int isi_cfm_mapinfo::get_version() const
{
	return version;
}

void isi_cfm_mapinfo::pack(struct isi_error **error_out)
{
	if (packed_map)
	{
		free(packed_map);
		packed_map = NULL;
	}

	pack(packed_size, &packed_map, error_out);
}

int isi_cfm_mapinfo::get_pack_size(isi_error **error_out) const
{
	int lsize = 0;
	isi_error *error = NULL;

	// Calculate the size of the header;
	lsize += get_pack_size_header();

	// Calculate size of the mapinfo entries and other info.
	lsize += get_pack_size_mapentries(&error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	isi_error_handle(error, error_out);
	return lsize;
}

int isi_cfm_mapinfo::get_pack_size_header(void) const
{
	int pack_header_size = 0;
	bool hasmdocheck = (type & ISI_CFM_MAP_TYPE_MAPCHECK);
	bool hascontainer_cmo = (type & ISI_CFM_MAP_TYPE_CONTAINER_CMO);

	CTASSERT(sizeof(pack_header_size) == sizeof(header_size));

	pack_header_size = sizeof(struct ifs_cpool_stubmap_info);

	pack_header_size += sizeof(type) + sizeof(count) + sizeof(chunksize) + sizeof(filesize) + sizeof(readsize) + sizeof(sparse_resolution) + sizeof(int) // placeholder for overall size
						+ policy_id.get_pack_size() + provider_id.get_pack_size();

	if (hascontainer_cmo)
	{
		pack_header_size += (sizeof(packingsz) + container_cmo.size());
		pack_header_size += account.get_pack_size();
		pack_header_size += object_id_.get_packed_size();
	}

	if (is_encrypted())
	{
		pack_header_size += sizeof(master_key_id);
		pack_header_size += sizeof(encrypted_key);
		pack_header_size += sizeof(master_iv);
	}

	if (hasmdocheck)
		pack_header_size += sizeof(checkinfo);

	return pack_header_size;
}

int isi_cfm_mapinfo::get_pack_size_mapentries(isi_error **error_out) const
{
	// note: don't change type/offset of this lsize field, we have
	// dependency on CMO streaming
	int lsize = 0;
	int entry_map_size = 0;
	struct isi_error *error = NULL;

	map_iterator it;

	// no over flow, pack the entries
	if (!is_overflow())
	{
		for (it = impl.begin(&error); it != impl.end();
			 it.next(&error))
		{
			entry_map_size = 0;
			it->second.get_pack_size(get_type(), entry_map_size,
									 false);
			lsize += entry_map_size;
		}
		ON_ISI_ERROR_GOTO(out, error, "Failed to iterate the mapinfo");
	}

out:
	isi_error_handle(error, error_out);
	return lsize;
}
int value_at_off(void *buf, int off = 0)
{
	return ((int *)((char *)buf + off))[0];
}
/*
 * Try and compress the mapinfo information into a bytesteam for sending to
 * the kernel.
 */
void isi_cfm_mapinfo::pack(int &my_packed_size, void **my_packed_map,
						   struct isi_error **error_out) const
{
	// note: don't change type/offset of this lsize field, we have
	// dependency on CMO streaming
	int lsize = 0;
	void *data = NULL;
	int magic = ISI_CFM_MAP_MAGIC;
	bool hasmdocheck = (type & ISI_CFM_MAP_TYPE_MAPCHECK);
	bool hascontainer_cmo = (type & ISI_CFM_MAP_TYPE_CONTAINER_CMO);
	struct isi_error *error = NULL;

	struct ifs_cpool_stubmap_info pack_header = {0};

	map_iterator it;
	int entry_map_size = 0;
	int pack_header_size;

	pack_header_size = get_pack_size_header();

	lsize = get_pack_size(&error);
//	ON_ISI_ERROR_GOTO(out, error);

	*my_packed_map = calloc(1, lsize);
	ASSERT(*my_packed_map);
	ASSERT(sizeof(lsize) == sizeof(size));
	data = *my_packed_map;

	pack_header.magic = magic;
	pack_header.version = version;
	pack_header.header_size = pack_header_size;

	packit(&data, &pack_header, sizeof(pack_header));

	packit(&data, &type, sizeof(type));
	packit(&data, &count, sizeof(count));
	packit(&data, &chunksize, sizeof(chunksize));
	packit(&data, &filesize, sizeof(filesize));
	packit(&data, &readsize, sizeof(readsize));
	packit(&data, &lsize, sizeof(lsize));

	/* pack policy_id and provider_id */
	policy_id.pack((char **)&data);
	provider_id.pack((char **)&data);

	/* pack size for contain_cmo if we have one */
	if (hascontainer_cmo)
	{
		short my_packingsz = container_cmo.size();
		packit(&data, &my_packingsz, sizeof(packingsz));
	}

	if (hascontainer_cmo)
	{
		packit(&data, container_cmo.c_str(), container_cmo.size());
		account.pack((char **)&data);
		object_id_.pack(&data);
	}

	// if (is_encrypted())
	// {
	// 	packit(&data, &master_key_id, sizeof(master_key_id));
	// 	packit(&data, &encrypted_key, sizeof(encrypted_key));
	// 	packit(&data, &master_iv, sizeof(master_iv));
	// }

	if (hasmdocheck)
		packit(&data, &checkinfo, sizeof(checkinfo));

	packit(&data, &sparse_resolution, sizeof(sparse_resolution));

	// no over flow, pack the entries
	if (!is_overflow())
	{
		for (it = impl.begin(&error); it != impl.end();
			 it.next(&error))
		{
			void *entry_map = NULL;
			entry_map_size = 0;

			it->second.pack(type, &entry_map, entry_map_size);
			packit(&data, entry_map, entry_map_size);
			if (entry_map != NULL)
			{
				free(entry_map);
			}
		}

		//ON_ISI_ERROR_GOTO(out, error, "Failed to iterate the mapinfo");
	}
	my_packed_size = lsize;

	int mapinfo_len = value_at_off(my_packed_map, 48);
	//printf("%s called my_packed_size:%ld mapinfo_len:%d\n", __func__, my_packed_size, mapinfo_len);
	//	ASSERT((int)(data - packed_map) == packed_size);

out:
	isi_error_handle(error, error_out);
}

#define MESSAGE_BUFFER_SIZE 512
static void log_assert(bool should_assert, const char *format, ...)
{
	char message[MESSAGE_BUFFER_SIZE];
	va_list ap;
	va_start(ap, format);
	vsnprintf(message, MESSAGE_BUFFER_SIZE, format, ap);
	va_end(ap);
	ilog(IL_ERR, message);
	if (should_assert)
		ASSERT(0);
}
bool isi_cfm_mapinfo::compare_fields(const isi_cfm_mapinfo &that,
									 bool should_assert) const
{
	if (version != that.version)
	{
		log_assert(should_assert,
				   "version field is different"
				   "my version = %d that version =%d",
				   version, that.version);
		return false;
	}

	if (header_size != that.header_size)
	{
		log_assert(should_assert, "header size is different"
								  "my header_size = %d that header_size = %d",
				   header_size,
				   that.header_size);
		return false;
	}

	if (type != that.type)
	{
		log_assert(should_assert, "type is different"
								  "my type = %d that type =%d",
				   type, that.type);
		return false;
	}
	if (count != that.count)
	{
		log_assert(should_assert, "count is different"
								  "my local_count = %d that local_count =%d",
				   count,
				   that.count);
		return false;
	}
	if (chunksize != that.chunksize)
	{
		log_assert(should_assert, "chunksize is different"
								  "my  = %d that  =%d",
				   chunksize, that.chunksize);
		return false;
	}
	if (size != that.size)
	{
		log_assert(should_assert, "size of the the packed map is different"
								  "my  = %d that  =%d",
				   chunksize, that.chunksize);
		return false;
	}
	if (readsize != that.readsize)
	{
		log_assert(should_assert, "readsize is different"
								  "my  = %d that  =%d",
				   readsize, that.readsize);
		return false;
	}
	if (sparse_resolution != that.sparse_resolution)
	{
		log_assert(should_assert, "sparse_resolution is different"
								  "my  = %d that  =%d",
				   sparse_resolution, that.sparse_resolution);
		return false;
	}

	return true;
}

#if 0  // not used at his point as all maps are stored via cpool syscall
/**
 * Method to take existing map context, binhex encode it and store it as
 * extended attribute on the file.
 */
bool
isi_cfm_mapinfo::write_map(int fd)
{
	int size  = 0;
	int write_size = 0;
	int error = -1;
	int written = 0;
	int ads_fd = -1;
	int attr_fd = -1;
	char *attr_buf = NULL;
	char tmpsz[16]; // big enough to hold the string representing the size

	/*
	 * pack the map for encoding and storing in the stub
	 */
	pack();

	/*
	 * Encode the data so it can be saved as an extended attribute.
	 * Cannot contain binary data.
	 */
	encode_map();

	/*
	 * The decode routine needs to know the target size for the decode.
	 * To do this we will store the size as a seperate attribute.
	 */
	sprintf(tmpsz, "%d", packed_size);

	size = strlen(tmpsz);

	ilog(IL_INFO, "writing %.*s to %s in namespace %d\n",
	    size, tmpsz, ISI_CFM_MAP_ATTR_SIZE,
	    ISI_CFM_MAP_ATTR_NAMESPACE);

	error = extattr_set_fd(fd, ISI_CFM_MAP_ATTR_NAMESPACE,
	    ISI_CFM_MAP_ATTR_SIZE, tmpsz, size);
	if (error != size) {
		/* error */
		fprintf(stderr, "extattr name write failed, attempted size was "
		    "%d, return was %d\n", size, error);

		error = -1;
		goto out;
	}

	/*
	 * Extended attributes have a fixed size, let's fit in as much as we
	 * can into the extended attribute
	 */
	write_size  = MIN(encoded_size, ISI_CFM_MAP_EXTATTR_THRESHOLD);
	ilog(IL_INFO, "writing %.*s to %s in namespace %d\n",
	    write_size, encoded_map, ISI_CFM_MAP_ATTR_NAME,
	    ISI_CFM_MAP_ATTR_NAMESPACE);
	error = extattr_set_fd(fd, ISI_CFM_MAP_ATTR_NAMESPACE,
	    ISI_CFM_MAP_ATTR_NAME, encoded_map, write_size);

	if (error < 0 ) {
		fprintf(stderr, "extattr write failed, attempted size was "
		    "%d, return was %d %s\n", write_size, error,
		    (error == -1) ? strerror(errno) : "");
		goto out;
	}

	/*
	 * Now check to see if all of the map fit into the extended attribute
	 * and if not, write the rest to an ADS if it is enabled.
	 */
	if (ISI_CFM_MAP_USE_ADS &&
	    error == write_size && encoded_size > write_size) {
		ads_fd = enc_openat(fd, "." , ENC_DEFAULT,
		    O_RDONLY|O_XATTR);
		if (ads_fd < 0) {
			fprintf(stderr, "ads dir open failed, return was %d "
			    "(%s)", errno, strerror(errno));
			error = -1;
			goto out;
		}

		/*
		 * The stream directory is now open/created. Now open the
		 * Stream itself.  Note that we use the CREAT and TRUNC
		 * options to make sure we can create it and if already exists,
		 * through out the old information.  We better not need this
		 * map anymore....
		 */
		attr_fd = enc_openat(ads_fd, ISI_CFM_MAP_ADS_NAME,
		    ENC_DEFAULT, O_CREAT|O_WRONLY|O_TRUNC);
		if (attr_fd < 0 ) {
			fprintf(stderr, "ads attr open failed, return was %d "
			    "(%s)", errno, strerror(errno));
			error = -1;
			goto out;
		}

		/*
		 * Now that we have the ADS open, write the rest of the map to
		 * it.  Treat the write just like any other file write.
		 */
		write_size = encoded_size - write_size;
		attr_buf = &encoded_map[write_size];
		while (write_size > 0) {
			written = write(attr_fd, attr_buf, write_size);
			if (written < 0) {
				fprintf(stderr," write to ads failed %d "
				    "(%s)\n", errno, strerror(errno));
				error = -1;
				break;
			} else {
				write_size -= written;
				attr_buf += written;
			}
		}
		close(attr_fd);
	} else {
		if (error == write_size)
			error = 0;
	}

 out:
	if (attr_fd != -1)
		close(attr_fd);
	if (ads_fd != -1)
		close(ads_fd);

	return (error == 0);
}
#endif /* 0 */

size_t
isi_cfm_mapinfo::get_total_size()
{
	size_t store_sz = 0;

	if (!packed_map)
		this->pack(isi_error_suppress());

	if (is_overflow())
	{
		// false: pack in extended attributes.
		// true: pack in sbt.
		int entry_size = 0;
		isi_cfm_mapentry::get_pack_size(get_type(), entry_size, false);
		store_sz = count * entry_size;
	}

	return packed_size + store_sz;
}

void isi_cfm_mapinfo::mark_mapping_type(isi_cfm_map_type map_type)
{
	type |= map_type;
}

/**
 * Use the cpool system call to store the stub map.  This system call allows
 * us to be able to perform all the stubbing operations under an exclusive
 * lock using the filerev as the validator.
 */
void /////把mapinfo的信息打包,写入到kernel中
isi_cfm_mapinfo::write_map_core(int fd, u_int64_t filerev,
								ifs_cpool_stub_opflags opflags, struct isi_error **error_out)
{
	size_t input_packed_size = 0;
	struct isi_error *error = NULL;

	ASSERT(*error_out == NULL);

	pack(&error);

	ON_ISI_ERROR_GOTO(out, error, "Failed to pack the mapinfo");

	input_packed_size = packed_size;

	if (ifs_cpool_stub_ops(fd, opflags, filerev, &input_packed_size,
						   packed_map, &input_packed_size) == -1)
	{
		switch (errno)
		{
		case ENOSYS:
			error = isi_cbm_error_new(CBM_NOSYS_ERROR,
									  "Unable to locate stub system call; lin %{}",
									  lin_fmt(lin));
			break;
		case EINVAL:
			error = isi_cbm_error_new(CBM_INVALID_PARAM_ERROR,
									  "Invalid stub request issued; lin %{}",
									  lin_fmt(lin));
			break;
		case EALREADY:
			error = isi_cbm_error_new(CBM_ALREADY_STUBBED,
									  "File already stubbed; lin %{}",
									  lin_fmt(lin));
			break;
		case EPERM:
			error = isi_cbm_error_new(CBM_PERM_ERROR,
									  "Insufficient privileges for stub request; "
									  "lin %{}",
									  lin_fmt(lin));
			break;
		case EOPNOTSUPP:
			error = isi_cbm_error_new(CBM_NOT_SUPPORTED_ERROR,
									  "Invalid mount type for stub request; lin %{}",
									  lin_fmt(lin));
			break;
		case EFTYPE:
			error = isi_cbm_error_new(CBM_FILE_TYPE_ERROR,
									  "Invalid stub request to nonregular file; "
									  "lin %{}",
									  lin_fmt(lin));
			break;
		case EROFS:
			error = isi_cbm_error_new(CBM_READ_ONLY_ERROR,
									  "Invalid attempt to stub read only FS; "
									  "lin %{}",
									  lin_fmt(lin));
			break;
		case ESTALE:
			error = isi_cbm_error_new(CBM_STALE_STUB_ERROR,
									  "File modified after stubbing began; lin %{}",
									  lin_fmt(lin));
			break;
		case EEXIST:
			error = isi_cbm_error_new(CBM_STUB_INVALIDATE_ERROR,
									  "Could not invalidate blocks for stub file; lin: "
									  "%{}",
									  lin_fmt(lin));
			break;
		default:
			error = isi_system_error_new(errno,
										 "Could not convert file to stub for lin %{}",
										 lin_fmt(lin));
			break;
		}
	}

out:
	isi_error_handle(error, error_out);
}

void isi_cfm_mapinfo::write_map(int fd, u_int64_t filerev,
								bool update, bool delayed_inval, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	ifs_cpool_stub_opflags opflags;

	if (update)
	{
		opflags = IFS_CPOOL_STUB_MAP_UPDATE;
	}
	else if (delayed_inval)
	{
		opflags = IFS_CPOOL_STUB_DELAY_INVAL;
	}
	else
		opflags = IFS_CPOOL_STUB_FULL;

	write_map_core(fd, filerev, opflags, &error);
	if (error)
		goto out;

out:
	isi_error_handle(error, error_out);
}

void isi_cfm_mapinfo::restore_map(int fd, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	uint32_t num_try = 0;

	// Restore stub map. Retry once to handle case where
	// filerev changes due to a delayed coalescer flush
	// See bug 117496 and 117538 for details
	do
	{
		if (error)
		{
			isi_error_free(error);
			error = NULL;
		}
		u_int64_t filerev;
		if (getfilerev(fd, &filerev))
		{
			error = isi_system_error_new(errno,
										 "Getting initial filerev for file");
			break;
		}
		write_map_core(fd, filerev, IFS_CPOOL_STUB_RESTORE, &error);
		num_try++;
	} while (error &&
			 isi_cbm_error_is_a(error, CBM_STALE_STUB_ERROR) && num_try < 2);

	if (error)
	{
		isi_error_add_context(error,
							  "Failed to restore stub mapinfo for file "
							  "after %u tries",
							  num_try);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

/*
 * unserialize the map and load the map object.  For the non-overflow case,
 * the mapentries will be unserialized as well.
 */
void isi_cfm_mapinfo::unpack(void *map, size_t buf_size, isi_error **error_out)
{
	unpack(map, buf_size, false, error_out);
}

void isi_cfm_mapinfo::unpack_header(void *map, isi_error **error_out)
{

	/* buf_size is set to 0 since we don't unpack anything but header */
	unpack(map, 0, true, error_out);
}

/*
 * unserialize the map and load the map object.  If header_only is specified
 * as true, any non-overflow mapentries will not be unpacked.
 */
void isi_cfm_mapinfo::unpack(void *map, size_t buf_size, bool header_only/*false*/,
							 isi_error **error_out)
{
	isi_error *error = NULL;
	void *data;
	int local_count;
	int magic;
	int i;
	int container_cmo_size;
	bool hasmdocheck;
	bool hascontainer_cmo;
	struct ifs_cpool_stubmap_info packed_header;

	ASSERT(sizeof(local_count) == sizeof(count));

	data = map;

	unpackit(&data, &packed_header, sizeof(packed_header));/////恢复mapinfo的头部
	if (packed_header.magic != ISI_CFM_MAP_MAGIC)
	{
		error = isi_system_error_new(EIO,
									 "The mapping info magic is not matched: expected "
									 "%x, got: %x",
									 ISI_CFM_MAP_MAGIC, packed_header.magic);
		goto out;
	}

	magic = packed_header.magic;
	version = packed_header.version;
	header_size = packed_header.header_size;

	if (CPOOL_CFM_MAP_VERSION != get_version())
	{
		error = isi_cbm_error_new(CBM_MAPINFO_VERSION_MISMATCH,
								  "The mapping info version mismatch: expected version %x, "
								  "found version: %x",
								  CPOOL_CFM_MAP_VERSION, get_version());
		goto out;
	}

	////恢复mapinfo的data部分
	unpackit(&data, &type, sizeof(type));///拷贝sizeof(type)个字节数给内存地址: &type

	hasmdocheck = (type & ISI_CFM_MAP_TYPE_MAPCHECK);
	hascontainer_cmo = (type & ISI_CFM_MAP_TYPE_CONTAINER_CMO);

	unpackit(&data, &local_count, sizeof(local_count));
	unpackit(&data, &chunksize, sizeof(chunksize));
	unpackit(&data, &filesize, sizeof(filesize));
	unpackit(&data, &readsize, sizeof(readsize));
	unpackit(&data, &size, sizeof(size));

	if (!header_only && (size_t)size > buf_size)
	{
		error = isi_system_error_new(EIO,
									 "The mapping info sizes is not matched: expected %ld, "
									 "got: %d",
									 buf_size, size);
		goto out;
	}

	policy_id.unpack((char **)&data);
	provider_id.unpack((char **)&data);

	if (hascontainer_cmo)
	{
		unpackit(&data, &packingsz, sizeof(packingsz));
		container_cmo_size = packingsz;
	}

	if (hascontainer_cmo)
	{
		if (container_cmo_size > 0)
		{
			container_cmo = container_cmo.assign((char *)data,
												 container_cmo_size);
			*(char **)&data += container_cmo_size;
		}
		account.unpack((char **)&data);
		object_id_.unpack(&data);
	}

	if (is_encrypted())
	{
		unpackit(&data, &master_key_id, sizeof(master_key_id));
		unpackit(&data, &encrypted_key, sizeof(encrypted_key));
		unpackit(&data, &master_iv, sizeof(master_iv));
	}

	if (hasmdocheck)
		unpackit(&data, &checkinfo, sizeof(checkinfo));

	unpackit(&data, &sparse_resolution, sizeof(sparse_resolution));

	if (is_overflow() || header_only)
	{
		/*
		 * in case of overflow or getting only the header, set the
		 * correct count from the header
		 */
		count = local_count;
	}
	else
	{
		// no overflow, unpack the entries
		for (i = 0; i < local_count; i++)
		{
			isi_cfm_mapentry mapentry;
			/*
			 * mapentry::unpack returns the next byte after the unpacked
			 * entry so we can start the next entry from there...
			 */
			data = mapentry.unpack(data, type, &error, false);
			ON_ISI_ERROR_GOTO(out, error);

			/*
			 * add the entry just like when we initially built the map.
			 */
			add(mapentry, &error);

			ON_ISI_ERROR_GOTO(out, error);
		}
	}

	if (count != local_count)
	{
		error = isi_cbm_error_new(CBM_PERM_ERROR,
								  "mapinfo object_id (%s, %lu): count %d local_count %d "
								  "mismatch ",
								  object_id_.to_c_string(), object_id_.get_snapid(),
								  count, local_count);
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

/*
 * This routine is called to verify the stubmap.  It should be called
 * after the complete stubmap (including the overflow SBT, if any) has
 * been constructed
 */
void isi_cfm_mapinfo::verify_map(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	iterator it;
	off_t expected_offset = 0;
	int deduced_count = 0;
	size_t deduced_filesize = 0;
	bool is_wip = (type & ISI_CFM_MAP_TYPE_WIP);

	// Check that we have contiguous, non-overlapping map entries
	for (it = impl.begin(&error); it != impl.end(); it.next(&error))
	{
		ON_ISI_ERROR_GOTO(out, error);

		isi_cfm_mapentry mapentry = it->second;

		if (mapentry.get_offset() % this->chunksize)
		{
			error = isi_cbm_error_new(CBM_PERM_ERROR,
									  "mapentry[%d] <%ld,%ld> snapid %lu for "
									  "mapinfo object_id (%s, %lu) is "
									  "not chunk aligned",
									  deduced_count, mapentry.get_offset(),
									  mapentry.get_length(),
									  mapentry.get_object_id().get_snapid(),
									  object_id_.to_c_string(),
									  object_id_.get_snapid());
			goto out;
		}

		if (mapentry.get_offset() != expected_offset)
		{
			error = isi_cbm_error_new(CBM_PERM_ERROR,
									  "mapentry[%d] <%ld,%ld> snapid %lu for "
									  "mapinfo object_id (%s, %lu) is not at "
									  "expected offset %ld",
									  deduced_count, mapentry.get_offset(),
									  mapentry.get_length(),
									  mapentry.get_object_id().get_snapid(),
									  object_id_.to_c_string(),
									  object_id_.get_snapid(),
									  expected_offset);
			goto out;
		}

		expected_offset = mapentry.get_offset() + mapentry.get_length();
		deduced_filesize += mapentry.get_length();
		deduced_count++;
	}

	// Check that the deduced count matches the one in the map
	if (deduced_count != count)
	{
		error = isi_cbm_error_new(CBM_PERM_ERROR,
								  "Expected %d mapentry found %d entries for "
								  "mapinfo object_id (%s, %lu)",
								  count, deduced_count,
								  object_id_.to_c_string(),
								  object_id_.get_snapid());
		goto out;
	}

	// As long as mapinfo is a work-in-progress, the check that follows
	// should not be done
	if (is_wip)
		goto out;

	// Check that deduced filesize matches the one in the map
	if (deduced_filesize != filesize)
	{
		error = isi_cbm_error_new(CBM_PERM_ERROR,
								  "Expected %lu filesize found %lu for "
								  "mapinfo object_id (%s, %lu)",
								  filesize, deduced_filesize,
								  object_id_.to_c_string(),
								  object_id_.get_snapid());
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

bool isi_cfm_mapinfo::has_same_cmo(const isi_cfm_mapinfo &other) const
{
	return has_same_cmo(other.get_account(), other.get_object_id());
}

bool isi_cfm_mapinfo::has_same_cmo(const isi_cbm_id &account,
								   const isi_cloud_object_id &object_id) const
{
	return get_account().equals(account) && get_object_id() == object_id;
}

void isi_cfm_mapinfo::truncate(off_t new_size, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (filesize <= new_size)
	{
		// only truncation down is supported.
		// extension shall be handled differently
		ilog(IL_DEBUG,
			 "mapinfo not truncated since filesize %ld <= new_size %ld",
			 filesize, new_size);
		return;
	}
	filesize = new_size;

	iterator end = impl.end();
	iterator begin = impl.begin(&error);
	iterator it = end;
	iterator ne;

	ON_ISI_ERROR_GOTO(out, error, "Failed to start the mapinfo iterator");

	it.prev(&error);
	ON_ISI_ERROR_GOTO(out, error, "Failed to move backward the mapinfo");

	while (it != end)
	{
		isi_cfm_mapentry entry = it->second;
		bool last = (it == begin);

		if (entry.get_offset() >= new_size)
		{
			if (!last)
			{
				ne = it.prev(&error);
				ON_ISI_ERROR_GOTO(out, error);

				it.next(&error);
				ON_ISI_ERROR_GOTO(out, error);
			}
			impl.erase(it, &error);
			ON_ISI_ERROR_GOTO(out, error,
							  "Failed to erase the entry");

			if (last)
				break;

			it = ne;
			continue;
		}
		else if (entry.get_offset() + (off_t)entry.get_length() >
				 new_size)
		{
			entry.set_length(new_size - entry.get_offset());

			impl.tx_erase(entry.get_offset());
			impl.tx_put(entry);
			impl.tx_commit(&error);
			ON_ISI_ERROR_GOTO(out, error);
		}
		else
			break;

		if (last)
			break;
		else
		{
			it.prev(&error);
			ON_ISI_ERROR_GOTO(out, error,
							  "Failed to move backward the mapinfo");
		}
	}

out:
	isi_error_handle(error, error_out);
}

bool isi_cfm_mapinfo::contains(off_t offset, const isi_cloud_object_id &id,
							   struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	bool contained = false;
	isi_cfm_mapinfo::iterator it;

	get_containing_map_iterator_for_offset(it, offset, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (it == end())
		goto out;

	if (it->second.get_object_id() == id)
		contained = true;

out:
	isi_error_handle(error, error_out);
	return contained;
}

int isi_cfm_mapinfo::get_upbound_pack_size()
{
	int sz = 0;
	isi_cfm_mapinfo *dummy = NULL;

	sz += sizeof(int); // magic
	sz += sizeof(dummy->version);
	sz += sizeof(dummy->type);
	sz += sizeof(dummy->count);
	sz += sizeof(dummy->chunksize);
	sz += sizeof(dummy->filesize);
	sz += sizeof(dummy->readsize);
	sz += sizeof(dummy->sparse_resolution);
	sz += sizeof(int);				   // lsize
	sz += isi_cbm_id::get_pack_size(); // policy id
	sz += isi_cbm_id::get_pack_size(); // provider id
	sz += sizeof(dummy->packingsz);	   //
	// container_cmo: m + guid str + i + %u, '%u' takes 10 chars at maximum
	sz += 1 + IFSCONFIG_GUID_STR_SIZE + 10;
	sz += isi_cbm_id::get_pack_size(); // account id
	sz += isi_cloud_object_id::get_packed_size();
	sz += sizeof(dummy->master_key_id);
	sz += sizeof(dummy->encrypted_key);
	sz += sizeof(dummy->master_iv);
	sz += sizeof(dummy->checkinfo);

	return sz;
}

int isi_cfm_mapinfo::get_size_offset()
{
	isi_cfm_mapinfo *dummy = NULL;
	struct ifs_cpool_stubmap_info header_info;
	// follows the packing sequence

	return sizeof(header_info) + sizeof(dummy->type) + sizeof(dummy->count) + sizeof(dummy->chunksize) + sizeof(dummy->filesize) + sizeof(dummy->readsize);
}

void isi_cfm_mapentry::setentry(uint32_t version, const isi_cbm_id &acct_id,
								off_t off, size_t len, std::string bucket, const isi_cloud_object_id &obj)
{
	ilog(IL_DEBUG, "sentry before: o,l,t = %ld,%ld\n", offset, length);
	version_ = version;
	account_id.copy(acct_id);
	offset = off;
	length = len;
	container = bucket;
	object_id_ = obj;
	ilog(IL_DEBUG, "sentry after: o,l,t = %ld,%ld\n", offset, length);
}

void isi_cfm_mapentry::setentry(uint32_t version, const isi_cbm_id &acct, off_t off,
								size_t len, std::string bucket, const isi_cloud_object_id &obj, ifs_guid checkinfo)
{
	version_ = version;
	account_id.copy(acct);
	offset = off;
	length = len;
	container = bucket;
	object_id_ = obj;
	check_info = checkinfo;
}

void isi_cfm_mapentry::pack(int itype, void **m_packed, int &my_packed_size,
							bool pack_overflow) const
{
	int alloc_size = 0;
	bool hascheckinfo = (itype & ISI_CFM_MAP_TYPE_CHECKINFO);
	bool hasobject = (itype & ISI_CFM_MAP_TYPE_OBJECT);
	void *data = NULL;

	get_pack_size(itype, alloc_size, pack_overflow);
	ASSERT(alloc_size > 0);

	*m_packed = (void *)calloc(1, alloc_size);

	ASSERT(*m_packed);

	my_packed_size = alloc_size;

	data = *m_packed;

	// Pack the version # if the entries are stored in the SBT.
	// Packing of version # is required only if entries are stored
	// in SBT. For other entries the version # of the map info will suffice.

	// IMPORTANT. To be done for future releases and for NDU purposes.
	// The version is packed for each map entry during overflow (when stored in sbt).
	// offset (not packed in sbt but in ea).
	// When streaming out to the cloud, the pack is done as if no overflow happened.
	// If we have map entries belonging to different versions in the sbt, then when streaming
	// out to the cloud, the map entries need to be normalized to be uniform through out.
	// The version in the header will be the version number of all entries.
	// Map entries of different versions in the SBT need to be made in sync in structure across
	// all entries when written to the cloud.
	if (pack_overflow)
		packit(&data, &version_, sizeof(version_));

	packit(&data, &alloc_size, sizeof(alloc_size));

	// optionally pack.
	// true: pack in extended attributes
	// false: pack in sbt.
	if (!pack_overflow)
		packit(&data, &offset, sizeof(offset));

	packit(&data, &length, sizeof(length));
	packit(&data, &plength, sizeof(plength));

	/*
	 * Add the strings to the packed data
	 */
	account_id.pack((char **)&data);

	if (hasobject)
	{
		object_id_.pack(&data);
		packit(&data, &index, sizeof(index));
	}

	if (hascheckinfo)
		packit(&data, &check_info, sizeof(check_info));
	// ASSERT((int)(data - packed_data) == packed_size);
}

void *
isi_cfm_mapentry::unpack(void *entry, int itype, isi_error **error_out,
						 bool pack_overflow)
{
	isi_error *error = NULL;
	void *data;
	int alloc_size;
	bool hascheckinfo = (itype & ISI_CFM_MAP_TYPE_CHECKINFO);
	bool hascontainer = (itype & ISI_CFM_MAP_TYPE_CONTAINER);
	bool hasobject = (itype & ISI_CFM_MAP_TYPE_OBJECT);

	data = entry;

	// The version # is saved if the entry is stored in the SBT.
	if (pack_overflow)
	{
		unpackit(&data, &version_, sizeof(version_));

		if (CPOOL_CFM_MAP_VERSION != get_version())
		{
			error = isi_system_error_new(EPROGMISMATCH,
										 "The mapping entry version mismatch: expected version %x, "
										 "found version: %x",
										 CPOOL_CFM_MAP_VERSION, get_version());
			goto out;
		}
	}

	unpackit(&data, &alloc_size, sizeof(alloc_size));
	set_packed_entry(entry, alloc_size);

	if (!pack_overflow)
		unpackit(&data, &offset, sizeof(offset));

	unpackit(&data, &length, sizeof(length));
	unpackit(&data, &plength, sizeof(plength));

	account_id.unpack((char **)&data);

	if (hascontainer)
	{
		// re-construct the container from the account id
		struct fmt FMT_INIT_CLEAN(bucket_fmt);

		format_bucket_name(bucket_fmt, account_id);

		container = fmt_string(&bucket_fmt);
	}

	if (hasobject)
	{
		object_id_.unpack(&data);
		unpackit(&data, &index, sizeof(index));
	}

	if (hascheckinfo)
		unpackit(&data, &check_info, sizeof(check_info));

out:
	isi_error_handle(error, error_out);
	return (data);
}

void isi_cfm_mapentry::get_pack_size(int itype, int &my_packed_size,
									 bool pack_overflow)
{
	isi_cfm_mapentry *dummy = NULL;
	int alloc_size = 0;
	bool hascheckinfo = (itype & ISI_CFM_MAP_TYPE_CHECKINFO);
	bool hasobject = (itype & ISI_CFM_MAP_TYPE_OBJECT);

	alloc_size = sizeof(alloc_size) + sizeof(dummy->length) +
				 sizeof(dummy->plength);

	// optionally pack.
	// false pack in extended attributes.
	// true pack in sbt.
	if (!pack_overflow)
	{
		alloc_size += sizeof(dummy->offset);
	}

	// Pack version # if the map entries are put in SBT.
	if (pack_overflow)
	{
		alloc_size += sizeof(dummy->version_);
	}

	alloc_size += dummy->account_id.get_pack_size();

	if (hasobject)
		alloc_size += dummy->object_id_.get_packed_size() +
					  sizeof(dummy->index);

	if (hascheckinfo)
		alloc_size += sizeof(dummy->check_info);

	my_packed_size = alloc_size;
}

/*
 * Utility function to initialize the master CDO information from the
 * mapping entry and the mapping information.
 * @param master_cdo[out] the master CDO
 * @param mapinfo[in] the map info object
 * @param entry[in] the mapping entry
 */
void master_cdo_from_map_entry(
	isi_cpool_master_cdo &master_cdo,
	const isi_cfm_mapinfo &mapinfo,
	const isi_cfm_mapentry *entry)
{
	bool concrete_master = false;

	cl_object_name obj_name = {
		.container_cmo = mapinfo.get_container(),
		.container_cdo = entry->get_container(),
		.obj_base_name = entry->get_object_id()};

	master_cdo.assign(obj_name,
					  entry->get_object_id().get_snapid(), mapinfo.get_chunksize(),
					  entry->get_offset(), entry->get_length(),
					  entry->get_index(),
					  NULL,
					  concrete_master);
	master_cdo.set_physical_length(entry->get_plength());
	master_cdo.set_readsize(mapinfo.get_readsize());
	master_cdo.set_sparse_resolution(
		mapinfo.get_sparse_resolution(), mapinfo.get_chunksize());
}

void isi_cbm_get_gcinfo_from_pq(const void *kernel_queue_entry,
								size_t kernel_queue_entry_size,
								ifs_lin_t &lin,
								ifs_snapid_t &snapid,
								isi_cfm_mapinfo &mapinfo,
								struct isi_error **error_out)
{

	struct ifs_kernel_delete_pq_info *kdi =
		(struct ifs_kernel_delete_pq_info *)kernel_queue_entry;
	struct ifs_cpool_stubmap_info *stubmap_header = &kdi->stubmap;
	struct isi_error *error = NULL;

	lin = kdi->lin;
	snapid = kdi->snap_id;

	mapinfo.unpack_header(stubmap_header, &error);
	ON_ISI_ERROR_GOTO(out, error);

	if (stubmap_header->header_size != mapinfo.get_headersize())
	{
		error = isi_cbm_error_new(CBM_STUBMAP_INVALID_HEADERSIZE,
								  "Header size in map %d does not match header size in"
								  " notification %d",
								  stubmap_header->header_size,
								  mapinfo.get_headersize());
	}

out:
	isi_error_handle(error, error_out);
}
