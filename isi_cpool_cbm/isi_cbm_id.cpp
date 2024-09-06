#include "isi_cpool_cbm/isi_cbm_id.h"

const isi_cbm_id isi_cbm_id::empty_id;

isi_cbm_id::isi_cbm_id()
{
	set_id(0);
	set_cluster_id("");
}

isi_cbm_id::isi_cbm_id(uint32_t id, const char *cluster_id)
{
	set_id(id);
	set_cluster_id(cluster_id);
}

bool
isi_cbm_id::equals(const isi_cbm_id &rid) const
{
	return (get_id() == rid.get_id() &&
	    strcmp(get_cluster_id(), rid.get_cluster_id()) == 0);
}

bool
isi_cbm_id::operator <(const isi_cbm_id &other) const
{
	int rslt = strcmp(get_cluster_id(), other.get_cluster_id());

	return rslt == 0 ?  (get_id() < other.get_id()) : rslt < 0;
}

void
isi_cbm_id::copy(const isi_cbm_id &rid)
{
	set_id(rid.get_id());
	set_cluster_id(rid.get_cluster_id());
}

void
isi_cbm_id::set_from_provider(const cpool_provider_instance &prov)
{
	set_id(prov.provider_instance_id);
	set_cluster_id(prov.birth_cluster_id);
}

void
isi_cbm_id::set_from_account(const cpool_account &acct)
{
	set_id(acct.account_id);
	set_cluster_id(acct.birth_cluster_id);
}

void
isi_cbm_id::set_from_policy(const fp_policy &pol)
{
	set_id(pol.id);
	set_cluster_id(pol.birth_cluster_guid);
}

void
isi_cbm_id::set_provider_from_policy(const fp_policy &pol)
{
	set_id(pol.attributes->cloudpool_action->cloud_provider_id);
	set_cluster_id(
	    pol.attributes->cloudpool_action->provider_birth_cluster);
}

void
isi_cbm_id::set_from_accountid(const cpool_accountid &aid)
{
	set_id(aid.account_id);
	set_cluster_id(aid.account_birth_cluster);
}


void
isi_cbm_id::set_cluster_id(const char *id)
{
	if (id == NULL || strlen(id) != IFSCONFIG_GUID_STR_SIZE - 1)
		bzero(cluster_id_, sizeof(cluster_id_));
	else
		strcpy(cluster_id_, id);
}

unsigned int
isi_cbm_id::get_pack_size()
{
	isi_cbm_id *dummy = NULL;

	return sizeof(dummy->id_) + IFSCONFIG_GUID_SIZE;
}

void
isi_cbm_id::get_cluster_id(uint8_t id[IFSCONFIG_GUID_SIZE]) const
{
	unsigned int c = 0;

	bzero(id, IFSCONFIG_GUID_SIZE);
	// convert 36 bytes hex back into 18 byte binary
	for (int i = 0; i < IFSCONFIG_GUID_SIZE; i++) {
		sscanf(&cluster_id_[i * 2], "%2x", &c);
		id[i] = c;
	}
}

