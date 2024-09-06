
#pragma once

#include "isi_cbm_mapinfo_store.h"

/**
 * mapinfo entry persistant in SBT implementation
 */
class isi_cfm_mapinfo_store_sbt : public isi_cfm_mapinfo_store
{
 public:
	isi_cfm_mapinfo_store_sbt(isi_cfm_mapinfo_store_cb *cb);

	~isi_cfm_mapinfo_store_sbt();

	/** @see isi_cbm_mapinfo_store::move_store() */
	void move(struct isi_error **error_out);

	/** @see isi_cbm_mapinfo_store::open() */
	void open(bool create_if_not_found, bool overwrite_if_exist, bool *created,
	    struct isi_error **error_out);

	/** @see isi_cbm_mapinfo_store::put() */
	void put(off_t off, isi_cfm_mapentry &entry,
	    struct isi_error **error_out);

	/** @see isi_cbm_mapinfo_store::put() */
	void put(isi_cfm_mapentry entries[], int cnt,
	    struct isi_error **error_out);

	/** @see isi_cbm_mapinfo_store::get() */
	bool get(off_t &off, isi_cfm_mapentry &entry, int opt,
	    struct isi_error **error_out) const;

	/** @see isi_cbm_mapinfo_store::get() */
	int get(off_t off, isi_cfm_mapentry entries[], int cnt,
	    struct isi_error **error_out) const;

	/** @see isi_cbm_mapinfo_store::remove() */
	void remove(off_t off, struct isi_error **error_out);

	/** @see isi_cbm_mapinfo_store::remove() */
	void remove(struct isi_error **error_out);

	/** @see isi_cbm_mapinfo_store::copy_from() */
	int copy_from(isi_cfm_mapinfo_store &src, struct isi_error **error_out);

	/** @see isi_cbm_mapinfo_store::equals() */
	bool equals(const isi_cfm_mapinfo_store &to,
	    struct isi_error **error_out);

	/** @see isi_cbm_mapinfo_store::exists() */
	bool exists(struct isi_error **error_out);

	/** @see isi_cbm_mapinfo_store::tx_commit() */
	int tx_commit(struct tx_op ops[], int nops,
	    struct isi_error **error_out);

	/** @see isi_cbm_mapinfo_store::get_store_path() */
	void get_store_path(std::string &str);

 private:
	bool remove_at(off_t off, struct isi_error **error_out);

	bool remove_all(struct isi_error **error_out);

	int open_folder(const std::string &fname,
	    bool create, struct isi_error **error_out);

	void hash_by_filename(const std::string &fname, char *l1, char *l2);

 private:
	int sbt_fd_;

	std::string sbt_fname_;
	std::string sbt_suffix_;

	isi_cfm_mapinfo_store_cb *cb_;
};
