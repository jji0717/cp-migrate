#ifndef __CBM_UNIT_TEST_HELPERS_H__
#define __CBM_UNIT_TEST_HELPERS_H__

#include <set>
#include <vector>
#include <string>
#include <list>

#include "isi_cbm_coi.h"
#include "isi_cbm_csi.h"
#include "isi_cbm_index_types.h"
#include "isi_cbm_ooi.h"
#include "isi_cbm_wbi.h"

class coi_entry_ut : public isi_cloud::coi_entry
{
public:
	const std::set<ifs_lin_t> &get_referencing_lins(void)
	{
		return referencing_lins_;
	}

	inline bool verbose_does_not_equal(const coi_entry_ut &rhs,
	    std::vector<std::string> &what_didnt_match) const;
};

class coi_ut : public isi_cloud::coi
{
public:
	isi_cloud::coi_entry *get_entry(isi_cloud_object_id object_id,
	    struct isi_error **error_out)
	{
		struct isi_error *error = NULL;
		isi_cloud::coi_entry *ent = NULL;

		ent = isi_cloud::coi::get_entry(object_id, &error);

		isi_error_handle(error, error_out);
		return ent;
	}

	int get_coi_sbt_fd(void)
	{
		return sbt_fd_;
	}
};

#define ADD_NON_MATCH(match, vec, what)		\
{						\
(match) = false;				\
(vec).push_back((what));			\
}

bool
coi_entry_ut::verbose_does_not_equal(const coi_entry_ut &rhs,
    std::vector<std::string> &what_didnt_match) const
{
	bool match = true;

	if (object_id_ != rhs.object_id_)
		ADD_NON_MATCH(match, what_didnt_match, "object_id_");

	if (version_ != rhs.version_)
		ADD_NON_MATCH(match, what_didnt_match, "version");

	if (memcmp(archiving_cluster_id_, rhs.archiving_cluster_id_,
	    IFSCONFIG_GUID_SIZE) != 0)
		ADD_NON_MATCH(match, what_didnt_match,
		    "archiving_cluster_id_");

	if (!archive_account_id_.equals(rhs.archive_account_id_))
		ADD_NON_MATCH(match, what_didnt_match, "archive_account_id_");

	if (strcmp(cmo_container_, rhs.cmo_container_) != 0)
		ADD_NON_MATCH(match, what_didnt_match,
		    "cmo_container_)");

	if (co_date_of_death_.tv_sec!= rhs.co_date_of_death_.tv_sec ||
		co_date_of_death_.tv_usec != rhs.co_date_of_death_.tv_usec)
		ADD_NON_MATCH(match, what_didnt_match, "co_date_of_death_");

	if (referencing_lins_.size() != rhs.referencing_lins_.size())
		ADD_NON_MATCH(match, what_didnt_match,
		    "referencing_lins_ (size)");
	std::set<ifs_lin_t>::const_iterator iter = referencing_lins_.begin();
	for (; iter != referencing_lins_.end(); ++iter)
		if (rhs.referencing_lins_.find(*iter) ==
		    rhs.referencing_lins_.end())
			ADD_NON_MATCH(match, what_didnt_match,
			    "referencing_lins_ (item)");

	return !match;
}

class ooi_entry_ut : public isi_cloud::ooi_entry
{
public:
      inline bool verbose_does_not_equal(const ooi_entry_ut &rhs,
          std::vector<std::string> &what_didnt_match) const;
};

bool
ooi_entry_ut::verbose_does_not_equal(const ooi_entry_ut &rhs,
    std::vector<std::string> &what_didnt_match) const
{
	bool match = true;

	if (version_ != rhs.version_)
		ADD_NON_MATCH(match, what_didnt_match, "version");

	if (items_.size() != rhs.items_.size())
		ADD_NON_MATCH(match, what_didnt_match,
		    "cloud_objects_ (size)");
	std::set<isi_cloud_object_id>::const_iterator iter = items_.begin();
	for (; iter != items_.end(); ++iter) {
		if (rhs.items_.find(*iter) == rhs.items_.end())
			ADD_NON_MATCH(match, what_didnt_match,
			    "cloud_objects_ (item)");
	}

	return !match;
}

class ooi_ut : public isi_cloud::ooi
{
public:
	int get_sbt_fd(void) const
	{
		return sbt_fd_;
	}

	isi_cloud::ooi_entry *
	_prepare_for_cloud_object_addition(struct timeval date_of_death,
	    uint32_t devid, const isi_cloud_object_id &object_id,
	    struct coi_sbt_bulk_entry &coi_sbt_op,
	    struct isi_error **error_out)
	{
		return isi_cloud::ooi::_prepare_for_cloud_object_addition(
		    date_of_death, devid, object_id, coi_sbt_op, error_out);
	}

	isi_cloud::ooi_entry *
	get_entry(struct timeval date_of_death, uint32_t devid, uint32_t index,
	    struct isi_error **error_out)
	{
		return isi_cloud::ooi::get_entry(date_of_death, devid, index,
		    error_out);
	}

	isi_cloud::ooi_entry *get_entry_before_or_at(time_t upper_limit,
	    bool &skipped_locked_entry_group, struct isi_error **error_out)
	{
		return isi_cloud::ooi::get_entry_before_or_at(upper_limit,
		    skipped_locked_entry_group, error_out);
	}

	void bulk_remove_and_add_entries(isi_cloud::ooi_entry *ent_to_remove,
	    isi_cloud::ooi_entry *ent_to_add, struct isi_error **error_out)
	{
		return isi_cloud::ooi::bulk_remove_and_add_entries(
		    ent_to_remove, ent_to_add, error_out);
	}
};

class csi_entry_ut : public isi_cloud::csi_entry
{
public:
	inline bool verbose_does_not_equal(const csi_entry_ut &rhs,
	    std::vector<std::string> &what_didnt_match) const;
};

bool
csi_entry_ut::verbose_does_not_equal(const csi_entry_ut &rhs,
    std::vector<std::string> &what_didnt_match) const
{
	bool match = true;

	if (version_ != rhs.version_)
		ADD_NON_MATCH(match, what_didnt_match, "version");

	if (items_.size() != rhs.items_.size())
		ADD_NON_MATCH(match, what_didnt_match, "cached_files_ (size)");
	std::set<isi_cloud::lin_snapid>::const_iterator iter = items_.begin();
	for (; iter != items_.end(); ++iter) {
		if (rhs.items_.find(*iter) == rhs.items_.end()) {
			ADD_NON_MATCH(match, what_didnt_match,
			    "cached_files_ (item)");
			break;
		}
	}

	return !match;
}

class csi_ut : public isi_cloud::csi
{
public:
	int get_sbt_fd(void) const
	{
		return sbt_fd_;
	}

	isi_cloud::csi_entry *
	get_entry(struct timeval process_time, uint32_t devid, uint32_t index,
	    struct isi_error **error_out)
	{
		return isi_cloud::csi::get_entry(process_time, devid, index,
		    error_out);
	}

	isi_cloud::csi_entry *get_entry_before_or_at(time_t upper_limit,
	    bool &skipped_locked_entry_group, struct isi_error **error_out)
	{
		return isi_cloud::csi::get_entry_before_or_at(upper_limit,
		    skipped_locked_entry_group, error_out);
	}

	void bulk_remove_and_add_entries(isi_cloud::csi_entry *ent_to_remove,
	    isi_cloud::csi_entry *ent_to_add, struct isi_error **error_out)
	{
		return isi_cloud::csi::bulk_remove_and_add_entries(
		    ent_to_remove, ent_to_add, error_out);
	}
};

class wbi_entry_ut : public isi_cloud::wbi_entry
{
public:
	inline bool verbose_does_not_equal(const wbi_entry_ut &rhs,
	    std::vector<std::string> &what_didnt_match) const;
};

bool
wbi_entry_ut::verbose_does_not_equal(const wbi_entry_ut &rhs,
    std::vector<std::string> &what_didnt_match) const
{
	bool match = true;

	if (version_ != rhs.version_)
		ADD_NON_MATCH(match, what_didnt_match, "version");

	if (items_.size() != rhs.items_.size())
		ADD_NON_MATCH(match, what_didnt_match, "cached_files_ (size)");
	std::set<isi_cloud::lin_snapid>::const_iterator iter = items_.begin();
	for (; iter != items_.end(); ++iter) {
		if (rhs.items_.find(*iter) == rhs.items_.end()) {
			ADD_NON_MATCH(match, what_didnt_match,
			    "cached_files_ (item)");
			break;
		}
	}

	return !match;
}

class wbi_ut : public isi_cloud::wbi
{
public:
	int get_sbt_fd(void) const
	{
		return sbt_fd_;
	}

	isi_cloud::wbi_entry *
	get_entry(struct timeval process_time, uint32_t devid, uint32_t index,
	    struct isi_error **error_out)
	{
		return isi_cloud::wbi::get_entry(process_time, devid, index,
		    error_out);
	}

	isi_cloud::wbi_entry *get_entry_before_or_at(time_t upper_limit,
	    bool &skipped_locked_entry_group, struct isi_error **error_out)
	{
		return isi_cloud::wbi::get_entry_before_or_at(upper_limit,
		    skipped_locked_entry_group, error_out);
	}

	void bulk_remove_and_add_entries(isi_cloud::wbi_entry *ent_to_remove,
	    isi_cloud::wbi_entry *ent_to_add, struct isi_error **error_out)
	{
		return isi_cloud::wbi::bulk_remove_and_add_entries(
		    ent_to_remove, ent_to_add, error_out);
	}
};

#endif // __CBM_UNIT_TEST_HELPERS_H__
