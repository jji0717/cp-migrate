#ifndef __ISI_CBM_ID__H__
#define __ISI_CBM_ID__H__

#include <isi_cpool_config/cpool_config.h>
#include <isi_pools_fsa_gcfg/isi_pools_fsa_gcfg.h>
#include "isi_cpool_cbm.h"

/**
 * Simple class for managing ID/cluster_id pairs
 *   Provides copy and comparison utilities as well as several setter functions
 *   for id/cluster_id population from various gconfig constructs.
 *   Should be easily convertible to isi_cbm_account_id and isi_cbm_policy_id
 *   as exposed in the C-API interface header (isi_cpool_cbm_c.h)
 */
class isi_cbm_id {
public:
	isi_cbm_id();
	isi_cbm_id(uint32_t id, const char *cluster_id);
	inline isi_cbm_id(const isi_cbm_id &original) { copy(original); };

	inline bool is_empty() const { return equals(empty_id); };
	bool equals(const isi_cbm_id &rid) const;
	void copy(const isi_cbm_id &rid);

	isi_cbm_id & operator = (const isi_cbm_id &original)
	{
		copy(original);
		return *this;
	}
	/**
	 * Use a given provider's provider_instance_id and birth_cluster_id to
	 * populate id and cluster_id, respectively
	 */
	void set_from_provider(const cpool_provider_instance &prov);
	/**
	 * Use a given account's account_id and birth_cluster_id to
	 * populate id and cluster_id, respectively
	 */
	void set_from_account(const cpool_account &acct);
	/**
	 * Use a given policy's id and birth_cluster_guid to
	 * populate id and cluster_id, respectively
	 */
	void set_from_policy(const fp_policy &pol);

	/**
	 * Use a given cpool_accountid's account_id and account_birth_cluster to
	 * populate id and cluster_id, respectively
	 * Note* cpool_accountid is used to store account info in providers
	 */
	void set_from_accountid(const cpool_accountid &aid);
	/**
	 * Use the cloud_provider_id and provider_birth_cluster specified in a
	 * given policy to populate id and cluster_id
	 */
	void set_provider_from_policy(const fp_policy &pol);

	inline void set_id(uint32_t id) { id_ = id; };
	inline uint32_t get_id() const { return id_; };

	void set_cluster_id(const char *id);

	inline const char *get_cluster_id() const { return cluster_id_; };

	/**
	 * Get the cluser id in binary format
	 */
	void get_cluster_id(uint8_t id[IFSCONFIG_GUID_SIZE]) const;

	/**
	 * Returns the number of bytes from this object which must be serialized
	 * E.g., the sizeof the ID plus the length of the cluster_id
	 */
	static unsigned int get_pack_size();

	/**
	 * An empty isi_cbm_id instance for use as a quasi-null value wherever
	 * needed
	 */
	static const isi_cbm_id empty_id;

	void pack(char **str) const
	{
		unsigned int c = 0;

		memcpy(*str, &id_, sizeof(id_));
		*str += sizeof(id_);

		bzero(*str, IFSCONFIG_GUID_SIZE);
		// convert 36 bytes hex back into 18 byte ascii
		for (int i = 0; i < IFSCONFIG_GUID_SIZE; i++) {
			sscanf(&cluster_id_[i * 2], "%2x", &c);
			(*str)[i] = c;
		}

		*str += IFSCONFIG_GUID_SIZE;
	}

	void unpack(char **str)
	{
		memcpy(&id_, *str, sizeof(id_));
		*str += sizeof(id_);

		bzero(cluster_id_, sizeof(cluster_id_));
		// ascii to hex in string
		for (int i = 0; i < IFSCONFIG_GUID_SIZE; i++) {
			unsigned char c = (*(unsigned char **) str)[i];
			sprintf(cluster_id_ + i * 2, "%02x", c);
		}

		// fixed 18 byte packed guid
		*str += IFSCONFIG_GUID_SIZE;
	}

	void get_cluster_id_in_bin(char buf[]) const {
		unsigned int c = 0;

		bzero(buf, IFSCONFIG_GUID_SIZE);
		for (int i = 0; i < IFSCONFIG_GUID_SIZE; ++i) {
			sscanf(&cluster_id_[i * 2], "%2x", &c);
			buf[i] = c;
		}
	}

	void set_cluster_id_in_bin(const char buf[]) {
		bzero(cluster_id_, sizeof(cluster_id_));

		// ascii to hex in string
		for (int i = 0; i < IFSCONFIG_GUID_SIZE; ++i) {
			unsigned char c = buf[i];
			sprintf(cluster_id_ + i * 2, "%02x", c);
		}
	}

	bool operator <(const isi_cbm_id &other) const;

	bool operator ==(const isi_cbm_id &other) const {
		return id_ == other.id_ && !strncmp(cluster_id_,
		    other.cluster_id_, IFSCONFIG_GUID_STR_SIZE);
	}

	bool operator !=(const isi_cbm_id &other) const {
		return !(*this == other);
	}

private:
	uint32_t id_;
	cluster_guid cluster_id_;
};

#endif // __ISI_CBM_ID__H__
