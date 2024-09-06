/**
 * @file
 *
 * Azure IO helper class for Cloud IO for CBM layer
 *
 * Copyright (c) 2012-2013
 * EMC Isilon Systems, Inc.  All rights reserved.
 *
 */

#include "ran_io_helper.h"
#include "isi_cbm_ioh_creator.h"
#include <openssl/md5.h>

using namespace isi_cloud;

namespace {

/**
 * Define the maximun number of directories in each level
 * of the CMO/CDO hash tree structure
 */
#define	RAN_DIR_HASH_LEVEL1		(200)
#define	RAN_DIR_HASH_LEVEL2		(200)

/**
 * Overlay the MD5 with 2 unsigned ints.
 */
typedef struct {
	union {
		unsigned char digest[MD5_DIGEST_LENGTH];
		struct {
			uint64_t	low;
			uint64_t	high;
		} i;
	} u;
} md5_hash_t;

/**
 * Routine to create an instance of azure io helper
 * @param acct[in] azure account
 * @return pointer to the created instance
 */
isi_cbm_ioh_base *
create_ran_io_helper(const isi_cbm_account &acct)
{
	return new ran_io_helper(acct);
}

/**
 * Register object that will register creation function to io helper
 * factory upon module loading
 */
isi_cbm_ioh_register g_reg(CPT_RAN, create_ran_io_helper);

}


ran_io_helper::ran_io_helper(const isi_cbm_account &acct) ///////传参数让基类初始化////先调用基类的构造函数class isi_cbm_ioh_base
	: isi_cbm_ioh_base(acct)
{
	ASSERT(acct.type == CPT_RAN,
	    "Only RAN should use ran_io_helper (%d requested)", acct.type);
	ran_p_ = &cl_provider_creator::get_provider(CPT_RAN);

	// no need to free ran provider pointer
	// ran_p_ = dynamic_cast<ran_provider *>(&prod);
	// ASSERT(ran_p_);
}

int
ran_io_helper::build_container_path(
    const std::string &container,
    const std::string &object_id,
    std::string &container_path)
{
	std::string hash_path;
	int new_levels = 0;

	container_path = "";
	if ((object_id.empty())) {
		if (!container.empty())
			container_path += container;
	} else {
                if (!container.empty())
			container_path += container + "/";

		hash_object_id(object_id, hash_path);
		new_levels += 2;
		container_path += hash_path;
	}
	return new_levels;
}

/*
 * Routine to build the directory hash string from the object_id
 */
void
ran_io_helper::hash_object_id(
    const std::string &object_id,
    std::string &hash_path)
{
	MD5_CTX 	md5_ctx;
	md5_hash_t	hash;
	unsigned int	level1_dir_id;
	unsigned int	level2_dir_id;
	char 		str_buf[12];

	MD5_Init(&md5_ctx);
	MD5_Update(&md5_ctx, (const void *)object_id.c_str(), object_id.size());
	MD5_Final(hash.u.digest, &md5_ctx);

	level1_dir_id = (unsigned int)(hash.u.i.high % RAN_DIR_HASH_LEVEL1);
	level2_dir_id = (unsigned int)(hash.u.i.low  % RAN_DIR_HASH_LEVEL2);

	snprintf(str_buf, 12, "%04d/%04d", level1_dir_id, level2_dir_id);
	hash_path = str_buf;

	return;
}


