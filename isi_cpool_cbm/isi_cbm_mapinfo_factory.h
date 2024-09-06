
#pragma once

#include "isi_cbm_mapinfo_store_sbt.h"

class isi_cfm_mapinfo_store_factory
{
 public:
	virtual ~isi_cfm_mapinfo_store_factory() { };

	virtual isi_cfm_mapinfo_store *create(isi_cfm_mapinfo_store_cb *cb) = 0;
};

class isi_cfm_mapinfo_store_sbt_factory : public isi_cfm_mapinfo_store_factory
{
 public:
	isi_cfm_mapinfo_store *create(isi_cfm_mapinfo_store_cb *cb)
	{
		return new isi_cfm_mapinfo_store_sbt(cb);
	}
};
