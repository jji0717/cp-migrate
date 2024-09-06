#include "ecs_io_helper.h"
#include "isi_cbm_ioh_creator.h"

namespace {
isi_cbm_ioh_base *
create_ecs_io_helper(const isi_cbm_account &acct) {
	return new ecs_io_helper(acct);
}

isi_cbm_ioh_register g_reg(CPT_ECS, create_ecs_io_helper);
isi_cbm_ioh_register g_reg2(CPT_ECS2, create_ecs_io_helper);
}

ecs_io_helper::ecs_io_helper(const isi_cbm_account &acct)
	: s3_io_helper(acct)
{
	ASSERT(acct.type == CPT_ECS || acct.type == CPT_ECS2,
	    "Only ECS and ECS2 should use ecs_io_helper (%d requested)",
	    acct.type);
}
