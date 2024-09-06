#include <check.h>
#include <ifs/ifs_types.h>
#include <isi_config/array.h>
#include <isi_cpool_cbm/isi_cbm_mapper.h>
#include <isi_cpool_cbm/isi_cbm_mapinfo_store.h>
#include <isi_cpool_d_common/isi_cloud_object_id.h>
#include <isi_cloud_common/isi_cpool_version.h>

#include "isi_cbm_error.h"
#include "check_cbm_common.h"

#define SYSCTL_CMD(a,v)	(a) = (void *)(v); a##_sz = strlen(v);

#define ENABLE_KD_DEBUG "efs.bam.cpool.enable_kd_debug"

#define TEST_FILE_NAME  "/ifs/data/_mapper_header_test"

static int
set_bool_sysctl(const char *name, bool on)
{
	void *sysctl_cmd = NULL;
	size_t sysctl_cmd_sz = 0;
	const char *value = (on ? "1" : "0");

	SYSCTL_CMD(sysctl_cmd, value);
	return sysctlbyname(name, NULL, NULL, sysctl_cmd, sysctl_cmd_sz);
}

TEST_FIXTURE(setup_suite);
TEST_FIXTURE(cleanup_suite);

TEST_FIXTURE(setup_test);

SUITE_DEFINE_FOR_FILE(check_cbm_mapper,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = setup_suite,
    .suite_teardown = cleanup_suite,
    .test_setup = setup_test,
    .timeout = 60,
    .fixture_timeout = 1200);

static size_t chunksize = 1024;
static isi_cloud_object_id object_id;

static char g_guid_str[IFSCONFIG_GUID_STR_SIZE];
static uint32_t ver = CPOOL_CFM_MAP_VERSION;

#define CHUNKSIZE(x)	(chunksize*(x))
#define OBJECT1		object_id

#define ADDENTRY(ver, a, o, l, ob, error)	\
	entry = new isi_cfm_mapentry; \
	{ \
		struct fmt FMT_INIT_CLEAN(bucket_fmt); \
		format_bucket_name(bucket_fmt, a); \
		entry->setentry(ver, (a), CHUNKSIZE((o)), CHUNKSIZE((l)), \
		    fmt_string(&bucket_fmt), (ob)); \
	} \
	map.add(*entry, &error); \
	fail_if(error, "failed to add entry: %#{}", isi_error_fmt(error)); \
	delete entry;

#define GETOFF(e)  (e)->get_offset()
#define GETLEN(e)  (e)->get_length()
#define GETACCT(e) (e)->get_account_id()

#define VDATA(a,o,l,ob) \
	{(a), CHUNKSIZE((o)), CHUNKSIZE((l)), (ob)}


TEST_FIXTURE(setup_suite)
{
	struct isi_error *error = NULL;
	isi_cloud_object_id dummy_id(0, 0); // prevents bogus 80 bytes mem leak

	// get system guid
	IfsConfig ifsc;

	arr_config_load_as_ifsconf(&ifsc, &error);

	fail_if(error, "failed to get cluster guid: %{}", isi_error_fmt(error));

	for (int i = 0; i < IFSCONFIG_GUID_SIZE; i++)
		sprintf(g_guid_str + i * 2, "%02x", ifsc.guid[i]);

	ifsConfFree(&ifsc);

}

TEST_FIXTURE(cleanup_suite)
{
	set_bool_sysctl(ENABLE_KD_DEBUG, false);
}

TEST_FIXTURE(setup_test)
{
	object_id.generate();
}

static inline
void check_map_entry(isi_cfm_mapinfo &map, off_t offset, bool expect,
    off_t entry_off, size_t entry_len, isi_cbm_id &entry_acct)
{
	isi_cfm_mapentry *entry = NULL;
	isi_cfm_mapinfo::iterator iter;
	struct isi_error *error = NULL;

	map.get_containing_map_iterator_for_offset(
	    iter, CHUNKSIZE(offset), &error);
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	if (!expect) {
		fail_if(iter != map.end(), "Unexpected entry found for offset %ld",
		    CHUNKSIZE(offset));
		return;
	}

	fail_if(iter == map.end(),
	    "No entry found for offset %ld, looking for (%ld, %ld %d)",
	    CHUNKSIZE(offset), CHUNKSIZE(entry_off), CHUNKSIZE(entry_len),
	    entry_acct.get_id());

	entry = &iter->second;
	fail_if(!(GETOFF(entry) == (off_t)CHUNKSIZE(entry_off) &&
		GETLEN(entry) == CHUNKSIZE(entry_len) &&
		GETACCT(entry).equals(entry_acct)),
	    "Found wrong entry for offset %ld found (%ld, %ld, %d), " \
	    "expected (%ld, %ld %d)",
	    CHUNKSIZE(offset), GETOFF(entry), GETLEN(entry),
	    GETACCT(entry).get_id(), CHUNKSIZE(entry_off), CHUNKSIZE(entry_len),
	    entry_acct.get_id());

	return;
}

static void
add_records_helper(isi_cfm_mapinfo &map)
{

	isi_cfm_mapentry *entry = NULL;
	struct isi_error *error = NULL;

	isi_cbm_id acct(1234, g_guid_str);
	isi_cbm_id acct2(2345, g_guid_str);
	int i = 0;

	struct verifyer v1[1] = {
		VDATA(acct, 1 , 1, OBJECT1)};

	ADDENTRY(ver, acct, 1, 1, OBJECT1, error); // 1
	map.dump();
	i++;
	fail_if(!map.verify(v1,sizeof(v1)/sizeof(struct verifyer)),
	    "Map %d does not match\n", i);

	struct verifyer v2[1] = {
		VDATA(acct, 0, 2, OBJECT1)};

	ADDENTRY(ver, acct, 0, 1, OBJECT1, error); // 2
	i++;
	fail_if(!map.verify(v2,sizeof(v2)/sizeof(struct verifyer)),
	    "Map %d does not match\n", i);

	struct verifyer v3[1] = {
		VDATA(acct, 0, 2, OBJECT1)};

	ADDENTRY(ver, acct, 1, 1, OBJECT1, error); // 3
	i++;
	fail_if(!map.verify(v3,sizeof(v3)/sizeof(struct verifyer)),
	    "Map %d does not match\n", i);

	struct verifyer v4[2] = {
		VDATA(acct, 0, 2, OBJECT1),
		VDATA(acct, 5, 1, OBJECT1)};

	ADDENTRY(ver, acct, 5, 1, OBJECT1, error); // 4
	i++;
	fail_if(!map.verify(v4,sizeof(v4)/sizeof(struct verifyer)),
	    "Map %d does not match\n", i);

	struct verifyer v5[2] = {
		VDATA(acct, 0, 3, OBJECT1),
		VDATA(acct, 5, 1, OBJECT1)};

	ADDENTRY(ver, acct, 2, 1, OBJECT1, error); // 5
	i++;
	fail_if(!map.verify(v5,sizeof(v5)/sizeof(struct verifyer)),
	    "Map %d does not match\n", i);

	struct verifyer v6[2] = {
		VDATA(acct, 0, 3, OBJECT1),
		VDATA(acct, 4, 2, OBJECT1)};

	ADDENTRY(ver, acct, 4, 1, OBJECT1, error); // 6
	i++;
	fail_if(!map.verify(v6,sizeof(v6)/sizeof(struct verifyer)),
	    "Map %d does not match\n", i);

	struct verifyer v7[2] = {
		VDATA(acct, 0, 3, OBJECT1),
		VDATA(acct, 4, 3, OBJECT1)};

	ADDENTRY(ver, acct, 6, 1, OBJECT1, error); // 7
	i++;
	fail_if(!map.verify(v7,sizeof(v7)/sizeof(struct verifyer)),
	    "Map %d does not match\n", i);

	struct verifyer v8[1] = {
		VDATA(acct, 0, 7, OBJECT1)};

	ADDENTRY(ver, acct, 3, 1, OBJECT1, error); // 8
	i++;
	fail_if(!map.verify(v8,sizeof(v8)/sizeof(struct verifyer)),
	    "Map %d does not match\n", i);

	struct verifyer v9[2] = {
		VDATA(acct, 0, 5, OBJECT1),
		VDATA(acct2, 5, 2, OBJECT1)};

	ADDENTRY(ver, acct2, 5, 2, OBJECT1, error);// 9
	i++;
	fail_if(!map.verify(v9,sizeof(v9)/sizeof(struct verifyer)),
	    "Map %d does not match\n", i);

	struct verifyer v10[3] = {
		VDATA(acct2, 0, 1, OBJECT1),
		VDATA(acct, 1, 4, OBJECT1),
		VDATA(acct2, 5, 2, OBJECT1)};

	ADDENTRY(ver, acct2, 0, 1, OBJECT1, error);// 10
	i++;
	fail_if(!map.verify(v10,sizeof(v10)/sizeof(struct verifyer)),
	    "Map %d does not match\n", i);

	struct verifyer v11[3] = {
		VDATA(acct2, 0, 1, OBJECT1),
		VDATA(acct, 1, 5, OBJECT1),
		VDATA(acct2, 6, 1, OBJECT1)};

	ADDENTRY(ver, acct, 5, 1, OBJECT1, error); // 11
	i++;
	fail_if(!map.verify(v11,sizeof(v11)/sizeof(struct verifyer)),
	    "Map %d does not match\n", i);

	// existing the last entry while also merging with its predecessor
	struct verifyer v12[2] = {
		VDATA(acct2, 0, 1, OBJECT1),
		VDATA(acct, 1, 7, OBJECT1)};
	ADDENTRY(ver, acct, 6, 2, OBJECT1, error); // 12
	i++;
	fail_if(!map.verify(v12,sizeof(v12)/sizeof(struct verifyer)),
	    "Map %d does not match\n", i);
}

TEST(add_records)
{
	isi_cfm_mapinfo map(chunksize, 999);

	add_records_helper(map);
}


TEST(check_header_2)
{
	isi_cfm_mapinfo map2_source(chunksize, 999);
	isi_cfm_mapinfo map2_target;
	isi_cfm_mapinfo map3_target;

	struct isi_error *error = NULL;

	size_t size1;
	size_t header_size;
	char header[8192];

	int fd = -1;
	int status = -1;
	u_int64_t filerev = 0;

	cbm_test_enable_stub_access();
	status = unlink(TEST_FILE_NAME);
	fail_if((status == -1 && errno != ENOENT),
	    "%s: Failed to cleanup old test file %s, %s (%d)",
	    __func__, TEST_FILE_NAME, strerror(errno), errno);

	map2_source.set_version(ISI_CFM_MAP_MIN_VERSION - 1);

	add_records_helper(map2_source);

	map2_source.set_convert_to_current(false);

	fd = open(TEST_FILE_NAME, O_CREAT|O_TRUNC|O_WRONLY, 0755);

	fail_if((fd == -1), "%s: Could not create testfile %s, %s (%d)",
	    __func__, TEST_FILE_NAME, strerror(errno), errno);

	status = getfilerev(fd, &filerev);
	fail_if((status == -1),
	    "%s: Could not get filerev for testfile %s, %s (%d)",
	    __func__, TEST_FILE_NAME, strerror(errno), errno);

	map2_source.write_map(fd, filerev, false, false, &error);
	fail_if(error, "%s: Cannot stub file %s: %#{}",
	    __func__, TEST_FILE_NAME, isi_error_fmt(error));

	set_bool_sysctl(ENABLE_KD_DEBUG, true);

	header_size = sizeof(header);
	status = ifs_cpool_stub_ops(fd, IFS_CPOOL_STUB_MAP_READ_HEADER,
	    0, &size1, header, &header_size);
	fail_if((status != -1),
	    "%s: should not be able to get header for version 2 stub",
	    __func__);
	fail_if((errno != EILSEQ),
	    "s: expected error to be EILSEQ (%d) received %d",
	    __func__, EILSEQ, errno);

	set_bool_sysctl(ENABLE_KD_DEBUG, false);

	if (fd != -1) {
		close(fd);
		fd = -1;
	}

	status = unlink(TEST_FILE_NAME);
	fail_if((status == -1),
	    "%s: Failed to cleanup test file %s, %s (%d)",
	    __func__, TEST_FILE_NAME, strerror(errno), errno);
	cbm_test_disable_stub_access();

}

TEST(check_header_3)
{
	isi_cfm_mapinfo map(chunksize, 999);

	struct isi_error *error = NULL;

	size_t size1;
	size_t header_size;
	size_t save_size;
	char header[8192];

	int fd = -1;
	int status = -1;
	u_int64_t filerev = 0;

	struct ifs_cpool_stubmap_info *orig_header;

	size_t compare_size;

	cbm_test_enable_stub_access();
	status = unlink(TEST_FILE_NAME);
	fail_if((status == -1 && errno != ENOENT),
	    "%s: Failed to cleanup old test file %s, %s (%d)",
	    __func__, TEST_FILE_NAME, strerror(errno), errno);

	add_records_helper(map);

	fd = open(TEST_FILE_NAME, O_CREAT|O_TRUNC|O_WRONLY, 0755);

	fail_if((fd == -1), "%s: Could not create testfile %s, %s (%d)",
	    __func__, TEST_FILE_NAME, strerror(errno), errno);

	status = getfilerev(fd, &filerev);
	fail_if((status == -1),
	    "%s: Could not get filerev for testfile %s, %s (%d)",
	    __func__, TEST_FILE_NAME, strerror(errno), errno);

	map.write_map(fd, filerev, false, false, &error);
	fail_if(error, "%s: Cannot stub file %s: %#{}",
	    __func__, TEST_FILE_NAME, isi_error_fmt(error));

	set_bool_sysctl(ENABLE_KD_DEBUG, true);

	/* 
	 * Try fetch with size < stubmap_info size, EINVAL expected
	 */
	header_size = 1;
	status = ifs_cpool_stub_ops(fd, IFS_CPOOL_STUB_MAP_READ_HEADER,
	    0, &size1, header, &header_size);
	fail_if((status != -1 || errno != EINVAL),
	    "%s: Expected failure of EINVAL on header fetch", __func__);

	/* 
	 * Try fetch where size is big enough for stubmap_info but less than
	 * what is needed for header itself, EOVERFLOW expected
	 */
	header_size = sizeof(struct ifs_cpool_stubmap_info);
	status = ifs_cpool_stub_ops(fd, IFS_CPOOL_STUB_MAP_READ_HEADER,
	    0, &size1, header, &header_size);
	fail_if((status != -1 || errno != EOVERFLOW),
	    "%s: Expected failure of EOVERFLOW on header fetch", __func__);

	/*
	 * fetch just the size by setting the stubmap header buffer to NULL
	 */
	header_size = sizeof(struct ifs_cpool_stubmap_info);
	status = ifs_cpool_stub_ops(fd, IFS_CPOOL_STUB_MAP_READ_HEADER,
	    0, &size1, NULL, &header_size);
	fail_if((status == -1),
	    "%s: could not get header size with NULL for stub %s, %s (%d)",
	    __func__, TEST_FILE_NAME, strerror(errno), errno);
	fail_if((header_size <= sizeof(struct ifs_cpool_stubmap_info)),
	    "%s: header size %ld less than minimum for stub %s",
	    __func__, header_size, TEST_FILE_NAME);

	save_size = header_size; /* save for comparison */

	/*
	 * fetch just size by setting header size to 0 but specifying header
	 */
	header_size = 0;
	status = ifs_cpool_stub_ops(fd, IFS_CPOOL_STUB_MAP_READ_HEADER,
	    0, &size1, header, &header_size);
	fail_if((status == -1),
	    "%s: could not get header size with size 0 for stub %s, %s (%d)",
	    __func__, TEST_FILE_NAME, strerror(errno), errno);
	fail_if((header_size <= sizeof(struct ifs_cpool_stubmap_info)),
	    "%s: header size %ld less than minimum for stub %s",
	    __func__, header_size, TEST_FILE_NAME);

	/*
	 * verify that the size returned for the two methods agree.
	 */
	fail_if((header_size != save_size),
	    "%s: size using NULL (%ld) does not match size using 0 (%ld)"
	    "for stub %s, %s (%d)",
	    __func__, save_size, header_size, TEST_FILE_NAME);

	/*
	 * Really get the header this time using the header size returned.
	 */
	status = ifs_cpool_stub_ops(fd, IFS_CPOOL_STUB_MAP_READ_HEADER,
	    0, &size1, header, &header_size);
	fail_if((status == -1),
	    "%s: could not get header for stub %s, %s (%d)",
	    __func__, TEST_FILE_NAME, strerror(errno), errno);

	fail_if((header_size != save_size),
	    "%s: size using NULL (%ld) does not match size returned"
	    " during real header fetch (%ld) for stub %s, %s (%d)",
	    __func__, save_size, header_size, TEST_FILE_NAME);

	orig_header = (struct ifs_cpool_stubmap_info *)map.get_packed_map();
	compare_size = orig_header->header_size;

	fail_if((header_size != compare_size),
	    "%s: packed header size of %d does not match retrieved size of %d",
	    __func__, compare_size, header_size);

	fail_if(memcmp(orig_header, header, compare_size),
	    "%s: map header does not match retrieved header for %s",
	    __func__, TEST_FILE_NAME);

	set_bool_sysctl(ENABLE_KD_DEBUG, false);

	if (fd != -1) {
		close(fd);
		fd = -1;
	}

	status = unlink(TEST_FILE_NAME);
	fail_if((status == -1),
	    "%s: Failed to cleanup test file %s, %s (%d)",
	    __func__, TEST_FILE_NAME, strerror(errno), errno);
	cbm_test_disable_stub_access();

}

TEST(add_records_overflow)
{
	struct isi_error *error = NULL;

	isi_cfm_mapentry *entry = NULL;
	isi_cfm_mapinfo map(chunksize, 999);
	isi_cbm_id acct1(1234, g_guid_str);
	isi_cbm_id acct2(2345, g_guid_str);

	struct verifyer v1[3] = {VDATA(acct1, 1, 1, OBJECT1)};
	struct verifyer v2[3] = {VDATA(acct1, 0, 2, OBJECT1)};
	struct verifyer v3[3] = {
	    VDATA(acct1, 0, 2, OBJECT1), VDATA(acct1, 5, 1, OBJECT1)};
	struct verifyer v4[3] = {
	    VDATA(acct1, 0, 3, OBJECT1), VDATA(acct1, 5, 1, OBJECT1)};
	struct verifyer v5[3] = {
	    VDATA(acct1, 0, 3, OBJECT1), VDATA(acct1, 4, 2, OBJECT1)};
	struct verifyer v6[3] = {
	    VDATA(acct1, 0, 3, OBJECT1), VDATA(acct1, 4, 3, OBJECT1)};
	struct verifyer v7[3] = {VDATA(acct1, 0, 7, OBJECT1)};
	struct verifyer v8[3] = {
	    VDATA(acct1, 0, 5, OBJECT1), VDATA(acct2, 5, 2, OBJECT1)};
	struct verifyer v9[3] = {
	    VDATA(acct2, 0, 1, OBJECT1), VDATA(acct1, 1, 4, OBJECT1),
	    VDATA(acct2, 5, 2, OBJECT1)};
	struct verifyer v10[3] = {
	    VDATA(acct2, 0, 1, OBJECT1), VDATA(acct1, 1, 5, OBJECT1),
	    VDATA(acct2, 6, 1, OBJECT1)};

	struct {
		isi_cbm_id &acct;
		isi_cloud_object_id &obj;
		off_t offset;
		size_t length;
		struct verifyer *v;
	} steps[] = {
	    {.acct = acct1, .obj = OBJECT1, .offset = 1, .length = 1, .v = v1},
	    {.acct = acct1, .obj = OBJECT1, .offset = 0, .length = 1, .v = v2},
	    {.acct = acct1, .obj = OBJECT1, .offset = 1, .length = 1, .v = v2},
	    {.acct = acct1, .obj = OBJECT1, .offset = 5, .length = 1, .v = v3},
	    {.acct = acct1, .obj = OBJECT1, .offset = 2, .length = 1, .v = v4},
	    {.acct = acct1, .obj = OBJECT1, .offset = 4, .length = 1, .v = v5},
	    {.acct = acct1, .obj = OBJECT1, .offset = 6, .length = 1, .v = v6},
	    {.acct = acct1, .obj = OBJECT1, .offset = 3, .length = 1, .v = v7},
	    {.acct = acct2, .obj = OBJECT1, .offset = 5, .length = 2, .v = v8},
	    {.acct = acct2, .obj = OBJECT1, .offset = 0, .length = 1, .v = v9},
	    {.acct = acct1, .obj = OBJECT1, .offset = 5, .length = 1, .v = v10}
	};

	std::vector<struct verifyer> v(map.get_overflow_threshold() + 1 + 3);

	// force overflow
	for (int i = 0; i < map.get_overflow_threshold() + 1; ++i) {
		ADDENTRY(ver, acct1, i * 2, 1, OBJECT1, error);
	}

	fail_if(!map.is_overflow(), "failed to overflow the entries");

	// initialize the verifyers
	for (int i = 0; i < map.get_overflow_threshold() + 1; ++i) {
		struct verifyer t = VDATA(acct1, i * 2, 1, OBJECT1);

		v[i] = t;
	}

	fail_if(!map.verify(v.data(), map.get_overflow_threshold() + 1),
	    "failed to verify the mapinfo");

	// test the consolidation and split case
	const off_t base_offset = 1024;
	for (unsigned i = 0; i < sizeof(steps) / sizeof(steps[0]); ++i) {
		int n = map.get_overflow_threshold() + 1;

		ADDENTRY(ver, steps[i].acct, base_offset + steps[i].offset,
		    steps[i].length, steps[i].obj, error);

		for (int j = 0; j < 3; ++j, ++n) {
			if (steps[i].v[j].account_id.get_id() == 0)
				break;

			v[n] = steps[i].v[j];
			v[n].offset += CHUNKSIZE(base_offset);
		}

		fail_if(!map.verify(v.data(), n), "failed to verify the mapinfo "
		    "at step %d", i);
	}
}

TEST(find_records)
{
	isi_cfm_mapinfo map(chunksize, 999);
	isi_cfm_mapentry *entry = NULL;
	struct isi_error *error = NULL;

	isi_cbm_id acct(1234, g_guid_str);
	isi_cbm_id acct2(2345, g_guid_str);
	isi_cbm_id acct3(3456, g_guid_str);
	isi_cbm_id acct4(4567, g_guid_str);

	ADDENTRY(ver, acct, 0, 2, OBJECT1, error); // 0 - 1
	ADDENTRY(ver, acct, 4, 1, OBJECT1, error); // 4
	ADDENTRY(ver, acct2, 5, 1, OBJECT1, error);// 5
	ADDENTRY(ver, acct3, 6, 2, OBJECT1, error);// 6 - 7
	ADDENTRY(ver, acct4, 8, 1, OBJECT1, error);// 8
	struct verifyer v[5] = {
		VDATA(acct, 0, 2, OBJECT1),
		VDATA(acct, 4, 1, OBJECT1),
		VDATA(acct2, 5, 1, OBJECT1),
		VDATA(acct3, 6, 2, OBJECT1),
		VDATA(acct4, 8, 1, OBJECT1),
	};
	fail_if(!map.verify(v,sizeof(v)/sizeof(struct verifyer)),
	    "Map for find does not match\n");

	check_map_entry(map, 0, true, 0, 2, acct);
/*	check_map_entry(map, 1, true, 0, 2, acct);
	check_map_entry(map, 2, false, 0, 2, acct);
	check_map_entry(map, 3, false, 0, 2, acct);
	check_map_entry(map, 4, true, 4, 1, acct);
	check_map_entry(map, 5, true, 5, 1, acct2);
	check_map_entry(map, 6, true, 6, 2, acct3);
	check_map_entry(map, 7, true, 6, 2, acct3);
	check_map_entry(map, 8, true, 8, 1, acct4);
	check_map_entry(map, 9, false, 8, 1, acct4);
*/
}

TEST(map_iterator_with_no_entries,  mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	isi_cfm_mapinfo map(chunksize, 0);
	struct isi_error *error = NULL;

	isi_cfm_mapinfo::map_iterator iter;
	isi_cfm_mapinfo::map_iterator iter1;

	map.get_map_iterator_for_offset(iter, 0, &error);

	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	map.get_containing_map_iterator_for_offset(iter1, 0, &error);
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
	fail_if(iter1 != map.end(), "Do not expect a mapping entry");
}

TEST(find_iterator)
{
	isi_cfm_mapinfo map(chunksize, 999);
	isi_cfm_mapentry *entry = NULL;
	struct isi_error *error = NULL;

	isi_cbm_id acct1(1234, g_guid_str);
	isi_cbm_id acct2(2345, g_guid_str);
	isi_cbm_id acct3(3456, g_guid_str);
	isi_cbm_id acct4(4567, g_guid_str);
	isi_cbm_id acct5(5678, g_guid_str);

	ADDENTRY(ver, acct1, 0, 1000, OBJECT1, error); // 0 - 1023999
	ADDENTRY(ver, acct2, 1000, 10, OBJECT1, error);// 1024000 - 1034239
	ADDENTRY(ver, acct3, 1010, 20, OBJECT1, error);//  1034240 - 1054719
	ADDENTRY(ver, acct4, 1030, 30, OBJECT1, error);//  1054720 - 1085439
	ADDENTRY(ver, acct5, 1060, 40, OBJECT1, error);//  1085440 - 1126399
	struct verifyer v[5] = {
		VDATA(acct1, 0, 1000, OBJECT1),
		VDATA(acct2, 1000, 10, OBJECT1),
		VDATA(acct3, 1010, 20, OBJECT1),
		VDATA(acct4, 1030, 30, OBJECT1),
		VDATA(acct5, 1060, 40, OBJECT1),
	};
	fail_if(!map.verify(v,sizeof(v)/sizeof(struct verifyer)),
	    "Map for find does not match\n");
	isi_cfm_mapinfo::iterator iter;
	off_t offset = 0;
	fail_if(!map.get_containing_map_iterator_for_offset(iter,offset, &error),
	    "Could not fimd entry for offset 0");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	fail_if(iter != map.begin(&error), "Test for offset 0 failed");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	offset = 1023999;
	fail_if(!map.get_containing_map_iterator_for_offset(iter,offset, &error),
	    "Could not fimd entry for offset 1023999");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	fail_if(iter != map.begin(&error), "Test for offset 1023999 failed");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	offset = 1024000;
	fail_if(!map.get_containing_map_iterator_for_offset(iter,offset, &error),
	    "Could not fimd entry for offset 1024000");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	fail_if(offset < iter->second.get_offset(),
	    "Found the wrong entry for offset 1024000 it is too small");
	fail_if((size_t)offset >=  iter->second.get_offset() +
	    iter->second.get_length(),
	    "Found wrong entry for offset 1024000 it is too big");

	isi_cfm_mapinfo::map_iterator my_iter = map.begin(&error);
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	my_iter.next(&error); // Now we are pointing to the 2nd rage.
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
	fail_if(iter != my_iter, "Test for offset 1024000 failed");

	for (int i = 2 ;iter != map.end(); iter.next(&error), i++) {
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
		fail_if(i > 5,
		    "This iterator cannot be used to iterate the list");
	}
	offset = 1126399;


	fail_if(!map.get_containing_map_iterator_for_offset(iter, offset, &error),
	    "Offset 1126399 is inside the map and should succeed!");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	offset = 1126400;


	fail_if(map.get_containing_map_iterator_for_offset(iter, offset, &error),
	    "Offset 1126400 is outside the map and should fail!");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
}

TEST(test_operators)
{
	// copy operation will not pack container/object id if it is default type
	// which fails the entry comparison
	// Also mark it a wip type to ensure the mapinfo.filesize does not
	// change per the mapentries (in the destination map)
	isi_cfm_mapinfo map(ISI_CFM_MAP_TYPE_OBJECTINFO | ISI_CFM_MAP_TYPE_WIP);
	isi_cfm_mapentry *entry = NULL;
	struct isi_error *error = NULL;

	map.set_chunksize(chunksize);
	map.set_filesize(999);

	isi_cbm_id acct1(1234, g_guid_str);
	isi_cbm_id acct2(2345, g_guid_str);
	isi_cbm_id acct3(3456, g_guid_str);
	isi_cbm_id acct4(4567, g_guid_str);
	isi_cbm_id acct5(5678, g_guid_str);

	// set the object is in case entries over flows
	map.set_object_id(object_id);

	ADDENTRY(ver, acct1, 0, 1000, OBJECT1, error); // 0 - 1023999
	ADDENTRY(ver, acct2, 1000, 10, OBJECT1, error); // 1024000 - 1034239
	ADDENTRY(ver, acct3, 1010, 20, OBJECT1, error);//  1034240 - 1054719
	ADDENTRY(ver, acct4, 1030, 30, OBJECT1, error);//  1054720 - 1085439
	ADDENTRY(ver, acct5, 1060, 40, OBJECT1, error);//  1085440 - 1126399
	struct verifyer v[] = {
		VDATA(acct1, 0, 1000, OBJECT1),
		VDATA(acct2, 1000, 10, OBJECT1),
		VDATA(acct3, 1010, 20, OBJECT1),
		VDATA(acct4, 1030, 30, OBJECT1),
		VDATA(acct5, 1060, 40, OBJECT1),
	};
	fail_if(!map.verify(v, sizeof(v) / sizeof(struct verifyer)),
	    "Map for find does not match\n");
	isi_cfm_mapinfo that_map(chunksize, 999);

	for (int i = 0; i < 10; i++) {
		that_map.copy(map, &error);
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

		fail_if(!that_map.equals(map, &error), "Maps are not equal "
		    " at iteration %d", i);

		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
	}

	// more entries
	for (int i = 0; i < 100; i++) {
		isi_cbm_id acct(i, "");
		ADDENTRY(ver, acct, 10000 + i * 100, 20, OBJECT1, error);
	}

	isi_cfm_mapinfo map_back(chunksize, 999);

	for (int i = 0; i < 10; i++) {
		map_back.copy(map, &error);
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

		fail_if(!map_back.equals(map, &error), "Maps are not equal ...");
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
	}

	//
	map.remove_store(&error);
	fail_if(error, "failed to clear mapinfo: %{}", isi_error_fmt(error));

	map_back.remove_store(&error);
	fail_if(error, "failed to clear mapinfo: %{}", isi_error_fmt(error));
}

TEST(test_add_overlap)
{
	isi_cfm_mapinfo map(ISI_CFM_MAP_TYPE_OBJECTINFO | ISI_CFM_MAP_TYPE_WIP);
	isi_cfm_mapentry *entry = NULL;
	struct isi_error *error = NULL;

	isi_cloud_object_id ids[6] = {
		isi_cloud_object_id(object_id),
		isi_cloud_object_id(object_id),
		isi_cloud_object_id(object_id),
		isi_cloud_object_id(object_id),
		isi_cloud_object_id(object_id),
		isi_cloud_object_id(object_id),
	};

	for (int i = 0; i < 6; ++i) {
		ids[i].set_snapid(i);
	}

	map.set_chunksize(chunksize);
	map.set_filesize(999);

	isi_cbm_id acct1(1234, g_guid_str);

	// set the object id in case entries over flows
	map.set_object_id(object_id);

	// Add an entry in the middle of another entry, splitting it
	ADDENTRY(ver, acct1, 0, 3, ids[0], error);
	ADDENTRY(ver, acct1, 1, 1, ids[1], error);

	{
		struct verifyer v[] = {
			VDATA(acct1, 0, 1, ids[0]),
			VDATA(acct1, 1, 1, ids[1]),
			VDATA(acct1, 2, 1, ids[0]),
		};
		fail_if(!map.verify(v, sizeof(v) / sizeof(struct verifyer)),
		    "Map does not match\n");
	}

	// Add an entry in the middle of another entry, merging with it
	ADDENTRY(ver, acct1, 4, 3, ids[0], error);
	ADDENTRY(ver, acct1, 5, 1, ids[0], error);

	{
		struct verifyer v[] = {
			VDATA(acct1, 0, 1, ids[0]),
			VDATA(acct1, 1, 1, ids[1]),
			VDATA(acct1, 2, 1, ids[0]),
			VDATA(acct1, 4, 3, ids[0]),
		};
		fail_if(!map.verify(v, sizeof(v) / sizeof(struct verifyer)),
		    "Map does not match\n");
	}

	// Merge with adjacent entries
	ADDENTRY(ver, acct1, 3, 1, ids[0], error);

	{
		struct verifyer v[] = {
			VDATA(acct1, 0, 1, ids[0]),
			VDATA(acct1, 1, 1, ids[1]),
			VDATA(acct1, 2, 5, ids[0]),
		};
		fail_if(!map.verify(v, sizeof(v) / sizeof(struct verifyer)),
		    "Map does not match\n");
	}

	ADDENTRY(ver, acct1, 7, 1, ids[2], error);
	ADDENTRY(ver, acct1, 8, 1, ids[3], error);
	ADDENTRY(ver, acct1, 9, 1, ids[4], error);

	{
		struct verifyer v[] = {
			VDATA(acct1, 0, 1, ids[0]),
			VDATA(acct1, 1, 1, ids[1]),
			VDATA(acct1, 2, 5, ids[0]),
			VDATA(acct1, 7, 1, ids[2]),
			VDATA(acct1, 8, 1, ids[3]),
			VDATA(acct1, 9, 1, ids[4]),
		};
		fail_if(!map.verify(v, sizeof(v) / sizeof(struct verifyer)),
		    "Map does not match\n");
	}


	// Replacing multiple overlapped entries
	ADDENTRY(ver, acct1, 7, 3, ids[5], error);

	{
		struct verifyer v[] = {
			VDATA(acct1, 0, 1, ids[0]),
			VDATA(acct1, 1, 1, ids[1]),
			VDATA(acct1, 2, 5, ids[0]),
			VDATA(acct1, 7, 3, ids[5]),
		};

		map.dump();
		map.verify_map(&error);
		fail_if(error, "Got error %{}", isi_error_fmt(error));

		fail_if(!map.verify(v, sizeof(v) / sizeof(struct verifyer)),
		    "Map does not match\n");
	}
}

TEST(test_verification_unaligned)
{
	isi_cfm_mapinfo map(ISI_CFM_MAP_TYPE_OBJECTINFO | ISI_CFM_MAP_TYPE_WIP);
	isi_cfm_mapentry *entry = NULL;
	struct isi_error *error = NULL;
	size_t save_chunksize = chunksize;

	map.set_chunksize(save_chunksize);
	map.set_filesize(999);

	isi_cbm_id acct1(1234, g_guid_str);
	isi_cbm_id acct2(2345, g_guid_str);

	// set the object id in case entries over flows
	map.set_object_id(object_id);

	// Add  entries
	chunksize = 7; // Change chunksize used by ADDENTRY below
	ADDENTRY(ver, acct1, 0, 1, OBJECT1, error);
	ADDENTRY(ver, acct2, 1, 10, OBJECT1, error); // !aligned to save_chunksize
	chunksize = save_chunksize;	// revert to saved

	map.verify_map(&error);
	fail_if(!error, "Expected error");
	fail_if(!isi_cbm_error_is_a(error, CBM_PERM_ERROR),
	    "Got %{}", isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;
}

TEST(test_verification_missing)
{
	isi_cfm_mapinfo map(ISI_CFM_MAP_TYPE_OBJECTINFO | ISI_CFM_MAP_TYPE_WIP);
	isi_cfm_mapentry *entry = NULL;
	struct isi_error *error = NULL;

	map.set_chunksize(chunksize);
	map.set_filesize(999);

	isi_cbm_id acct1(1234, g_guid_str);

	// set the object id in case entries over flows
	map.set_object_id(object_id);

	// Add non-contiguous entries
	ADDENTRY(ver, acct1, 0, 1000, OBJECT1, error); // 0 - 1023999
	ADDENTRY(ver, acct1, 1010, 20, OBJECT1, error);//  1034240 - 1054719

	map.verify_map(&error);
	fail_if(!error, "Expected error");
	fail_if(!isi_cbm_error_is_a(error, CBM_PERM_ERROR),
	    "Got %{}", isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;
}

TEST(test_verification_filesize)
{
	isi_cfm_mapinfo map(ISI_CFM_MAP_TYPE_OBJECTINFO);
	struct isi_error *error = NULL;

	map.set_chunksize(chunksize);
	map.set_filesize(999);

	isi_cbm_id acct1(1234, g_guid_str);

	// set the object id in case entries over flows
	map.set_object_id(object_id);

	// Verify.  This should fail since the map filesize does not
	// correspond to the size of the entries contained (0).
	map.verify_map(&error);
	fail_if(!error, "Expected error");
	fail_if(!isi_cbm_error_is_a(error, CBM_PERM_ERROR), "Got %{}",
	    isi_error_fmt(error));
	isi_error_free(error);
	error = NULL;

	map.set_type(map.get_type() | ISI_CFM_MAP_TYPE_WIP);

	// Verify again.  This should succeed since the map filesize
	// check is disregarded for wip map.
	map.verify_map(&error);
	fail_if(error, "Got error %{}", isi_error_fmt(error));
}


static bool offset_belongs(const isi_cfm_mapentry *entry, off_t offset)
{
	if (entry == NULL)
		return false;
	if (entry->get_offset() > offset)
		return false;
	if (entry->get_offset() + entry->get_length() <= (size_t) offset)
		return false;
	return true;
}

TEST(find_read_entry)
{
	isi_cfm_mapinfo map(chunksize, 999);
	isi_cfm_mapentry *entry = NULL;
	struct isi_error *error = NULL;

	isi_cbm_id acct1(1234, g_guid_str);
	isi_cbm_id acct2(2345, g_guid_str);
	isi_cbm_id acct3(3456, g_guid_str);
	isi_cbm_id acct4(4567, g_guid_str);
	isi_cbm_id acct5(5678, g_guid_str);

	ADDENTRY(ver, acct1, 0, 1000, OBJECT1, error); // 0 - 1023999
	ADDENTRY(ver, acct2, 1000, 10, OBJECT1, error); // 1024000 - 1034239
	ADDENTRY(ver, acct3, 1010, 20, OBJECT1, error);//  1034240 - 1054719
	ADDENTRY(ver, acct4, 1030, 30, OBJECT1, error);//  1054720 - 1085439
	ADDENTRY(ver, acct5, 1060, 40, OBJECT1, error);//  1085440 - 1126399
	struct verifyer v[5] = {
		VDATA(acct1, 0, 1000, OBJECT1),
		VDATA(acct2, 1000, 10, OBJECT1),
		VDATA(acct3, 1010, 20, OBJECT1),
		VDATA(acct4, 1030, 30, OBJECT1),
		VDATA(acct5, 1060, 40, OBJECT1),
	};
	fail_if(!map.verify(v,sizeof(v)/sizeof(struct verifyer)),
	    "Map for find does not match\n");
	isi_cfm_mapinfo::map_iterator iter;
	off_t offset = 0;
	map.get_containing_map_iterator_for_offset(iter, offset, &error);

	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
	fail_if(!offset_belongs(&iter->second, offset),
	    "Could not find entry for offset 0");

	fail_if(iter->first != 0,
	    "Entry for offset 0 is not the first entry.");

	offset = 1023999;
	map.get_containing_map_iterator_for_offset(iter, offset, &error);

	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
	fail_if(!offset_belongs(&iter->second, offset),
	    "Could not find entry for offset 1023999");
	fail_if(iter->first != 0,
	    "Test for offset 1023999 failed");


	offset = 1024000;
	map.get_containing_map_iterator_for_offset(iter, offset, &error);

	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
	fail_if(!offset_belongs(&iter->second, offset),
	    "Could not fimd entry for offset 1024000");

	offset = 1126399;
	map.get_containing_map_iterator_for_offset(iter, offset, &error);

	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
	fail_if(!offset_belongs(&iter->second, offset),
	    "Offset 1126399 is inside the map and should succeed!");

	offset = 1126400;
	map.get_containing_map_iterator_for_offset(iter, offset, &error);

	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
	fail_if(iter != map.end(),
	    "Offset 1126400 is outside the map and should fail!");
}

TEST(test_mapinfo_store, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	struct isi_error *error = NULL;

	isi_cfm_mapinfo mapinfo(ISI_CFM_MAP_TYPE_OBJECTINFO);
	isi_cfm_mapinfo_impl impl(&mapinfo);
	isi_cfm_mapinfo_store_factory *factory =
	    new isi_cfm_mapinfo_store_sbt_factory();

	fail_if(!factory, "failed to construct store factory");

	mapinfo.set_object_id(object_id);

	isi_cfm_mapinfo_store *store = factory->create(&impl);
	fail_if(!store, "failed to construct mapinfo store");

	store->open(true, false, NULL, &error);
	fail_if(error, "failed to open store: %{}", isi_error_fmt(error));

	// remove all the entries
	store->remove(-1, &error);
	fail_if(error, "failed to clean up the store: %{}",
	    isi_error_fmt(error));

	isi_cbm_id acct(1234, "");

	// initialize data
	for (int i = 0; i < 10; i++) {
		isi_cfm_mapentry entry;

		entry.setentry(ver,
		    acct, CHUNKSIZE(i), CHUNKSIZE(1), "", OBJECT1);

		store->put(CHUNKSIZE(i), entry, &error);

		fail_if(error, "failed to put (%ld %ld): %{}", CHUNKSIZE(i),
		    CHUNKSIZE(1), isi_error_fmt(error));
	}

	// check store->get
	for (int i = 0; i < 10; i++) {
		isi_cfm_mapentry entry;
		off_t offset = CHUNKSIZE(i);

		store->get(offset, entry, isi_cfm_mapinfo_store::AT, &error);

		fail_if(error, "failed to get entry %ld: %{}", offset,
		    isi_error_fmt(error));
		fail_if((size_t)offset != CHUNKSIZE(i), "offset not match");
	}

	{
		isi_cfm_mapinfo backup_mapinfo(ISI_CFM_MAP_TYPE_OBJECTINFO);
		isi_cfm_mapinfo_impl backup_impl(&backup_mapinfo);
		isi_cloud_object_id backup_object_id;

		backup_object_id.generate();
		backup_mapinfo.set_object_id(backup_object_id);

		// check copy
		isi_cfm_mapinfo_store *store_backup =
		    factory->create(&backup_impl);

		fail_if(!store_backup, "failed to construct mapinfo store");

		fail_if(error, "failed to open store: %{}",
		    isi_error_fmt(error));

		store_backup->open(true, false, NULL, &error);
		fail_if(error, "failed to open store: %{}",
		    isi_error_fmt(error));

		store_backup->copy_from(*store, &error);

		fail_if(error, "failed to copy the store: %{}",
		    isi_error_fmt(error));

		fail_if(!store_backup->equals(*store, &error),
		    "the copied store should be same to the source store");

		fail_if(error, "failed to compare two stores: %{}",
		    isi_error_fmt(error));

		// check remove
		store_backup->remove(&error);
		fail_if(error, "failed to remove store: %{}",
		    isi_error_fmt(error));
		fail_if(store_backup->exists(&error), "failed to remove sbt");
		fail_if(error, "unexpected error: %{}", isi_error_fmt(error));

		delete store_backup;
	}

	{
		isi_cfm_mapentry entry;
		bool res;
		off_t offset = CHUNKSIZE(6) + 10;

		// check store->get_before
		res = store->get(offset, entry,
		    isi_cfm_mapinfo_store::AT_OR_BEFORE, &error);

		fail_if(!res || offset != CHUNKSIZE(6)
		    || entry.get_offset() != offset,
		    "failed to get at or before the key in store");

		fail_if(error, "failed to get at or before the key in "
		    "store: %{}", isi_error_fmt(error));

		// check store->get_after
		offset = CHUNKSIZE(6) + 10;
		res = store->get(offset, entry,
		    isi_cfm_mapinfo_store::AT_OR_AFTER, &error);

		fail_if(!res || offset != CHUNKSIZE(7)
		    || entry.get_offset() != offset,
		    "failed to get at or after");

		fail_if(error, "failed to get at or after the key in "
		    "store: %{}", isi_error_fmt(error));
	}

	{
		// check bulk get entries
		isi_cfm_mapentry entries[4];

		fail_if(store->get(CHUNKSIZE(0), entries, 4, &error) != 4,
		    "failed to get entries");
		fail_if(error, "failed to get entries: %{}",
		    isi_error_fmt(error));

		fail_if(store->get(CHUNKSIZE(4), entries, 4, &error) != 4,
		    "failed to get entries");
		fail_if(error, "failed to get entries: %{}",
		    isi_error_fmt(error));

		fail_if(store->get(CHUNKSIZE(8), entries, 4, &error) != 2,
		    "failed to get entries");
		fail_if(error, "failed to get entries: %{}",
		    isi_error_fmt(error));
	}

	{
		isi_cfm_mapentry entry;
		off_t offset;

		// check delete
		store->remove(CHUNKSIZE(0), &error);
		fail_if(error, "failed to remove %ld: %{}", CHUNKSIZE(0),
		    isi_error_fmt(error));

		store->remove(CHUNKSIZE(9), &error);
		fail_if(error, "failed to remove %ld: %{}", CHUNKSIZE(9),
		    isi_error_fmt(error));

		offset = CHUNKSIZE(0);
		fail_if(store->get(offset, entry, isi_cfm_mapinfo_store::AT,
		    &error), "should not get %ld", CHUNKSIZE(0));
		fail_if(error, "should not get entry %ld", CHUNKSIZE(0));

		offset = CHUNKSIZE(9);
		fail_if(store->get(offset, entry, isi_cfm_mapinfo_store::AT,
		    &error), "should not get %ld", CHUNKSIZE(9));
		fail_if(error, "should not get %ld", CHUNKSIZE(9));
	}

	store->remove(&error);

	fail_if(error, "failed to remove the store: %{}", isi_error_fmt(error));

	delete store;
	delete factory;
}

TEST(test_mapinfo_impl, mem_check : CK_MEM_DEFAULT, mem_hint : 0)
{
	struct isi_error *error = NULL;
	isi_cbm_id acct(1234, g_guid_str);
	isi_cfm_mapinfo mapinfo(ISI_CFM_MAP_TYPE_OBJECT);
	mapinfo.set_object_id(object_id);

	isi_cfm_mapinfo_impl impl(&mapinfo);

	// check begin == end
	fail_if(impl.begin(&error) != impl.end(),
	    "begin() should equals to end() when it is empry");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	// check --end() == end()
	fail_if(impl.end().prev(&error) != impl.end(),
	    "--end() is supposed to be end() when map is empty");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	// add 50 entries
	for (int i = 0; i < mapinfo.get_overflow_threshold(); i++) {
		isi_cfm_mapentry entry;
		struct fmt FMT_INIT_CLEAN(bucket_fmt);

		entry.setentry(ver, acct, CHUNKSIZE(i), CHUNKSIZE(1),
		    fmt_string(&bucket_fmt), OBJECT1);

		impl.put(entry, &error);
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

		fail_if(!impl.is_in_memory(),
		    "%d-th entry is not supposed to overflow");

		fail_if(mapinfo.get_count() != i + 1, "count = %d, %d is expected",
		    mapinfo.get_count(), i + 1);
	}

	// check begin()/last()
	fail_if(impl.last(&error)->first !=
	    (off_t)CHUNKSIZE(mapinfo.get_overflow_threshold() - 1),
	    "failed to get last iterator");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	fail_if(impl.begin(&error)->first != CHUNKSIZE(0),
	    "failed to get begin iterator");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	fail_if(impl.begin(&error).prev(&error) != impl.begin(&error),
	    "--begin() is supposed to equal to begin()");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	fail_if(impl.end().next(&error) != impl.end(),
	    "++end() is supposed to equal to end()");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	// check copy
	{
		isi_cfm_mapinfo mapinfo_backup(ISI_CFM_MAP_TYPE_OBJECT);
		isi_cfm_mapinfo_impl impl_backup(&mapinfo_backup);

		impl_backup.copy_from(impl, &error);

		fail_if(error, "failed to copy the entries: %{}",
		    isi_error_fmt(error));

		fail_if(!impl_backup.equals(impl, &error),
		    "the copied map is supposed to be equal to the source");

		fail_if(error, "failed to compare the entries: %{}",
		    isi_error_fmt(error));

		impl_backup.remove(&error);
		fail_if(error, "failed to clear mapinfo: %{}",
		    isi_error_fmt(error));
	}

	// check find
	for (int i = 0; i < mapinfo.get_overflow_threshold(); i++) {
		fail_if(impl.find(CHUNKSIZE(i), &error) == impl.end(),
		    "failed to find offset %ld", CHUNKSIZE(i));
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

		fail_if(impl.find(CHUNKSIZE(i) - 10, &error) != impl.end(),
		    "not supposed to find offset %ld", CHUNKSIZE(i) - 10);
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

		fail_if(impl.find(CHUNKSIZE(i) + 10, &error) != impl.end(),
		    "not supposed to find offset %ld", CHUNKSIZE(i) + 10);
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
	}

	// iterate in-memory entries
	int idx = 0;

	for (isi_cfm_mapinfo_impl::iterator itr = impl.begin(&error);
	    itr != impl.end(); itr.next(&error), ++idx) {
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
		fail_if(itr->first != (off_t)CHUNKSIZE(idx),
		    "offset not match: %ld vs %ld",
		    itr->first, CHUNKSIZE(idx));

		fail_if(itr->second.get_offset() != (off_t)CHUNKSIZE(idx),
		    "offset not match: %ld vs %ld",
		    itr->second.get_offset(), CHUNKSIZE(idx));
	}
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	fail_if(idx != mapinfo.get_overflow_threshold(),
	    "iterate through %d entries, %d expected", idx,
	    mapinfo.get_overflow_threshold());

	// add another 100 entries
	const int EXTRA = 100;
	for (int i = mapinfo.get_overflow_threshold();
	    i < mapinfo.get_overflow_threshold() + EXTRA; i++) {
		isi_cfm_mapentry entry;

		entry.setentry(ver,
		    acct, CHUNKSIZE(i), CHUNKSIZE(1), "", OBJECT1);

		impl.put(entry, &error);

		fail_if(error, "failed to put (%ld %ld): %{}", CHUNKSIZE(i),
		    CHUNKSIZE(1), isi_error_fmt(error));

		fail_if(impl.is_in_memory(),
		    "%d-th entry is supposed to overflow to external store");

		fail_if(mapinfo.get_count() != i + 1,
		    "count = %d, %d is expected", mapinfo.get_count(), i + 1);
	}
	// check begin()/last()
	fail_if(impl.last(&error)->first !=
	    (off_t)CHUNKSIZE(mapinfo.get_overflow_threshold() + EXTRA - 1),
	    "failed to get last iterator");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	fail_if(impl.begin(&error)->first != CHUNKSIZE(0),
	    "failed to get begin iterator");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	fail_if(impl.begin(&error).prev(&error) != impl.begin(&error),
	    "--begin() is supposed to equal to begin()");
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	fail_if(impl.end().next(&error) != impl.end(),
	    "++end() is supposed to equal to end()");

	// check find
	for (int i = 0; i < mapinfo.get_overflow_threshold() + EXTRA; i++) {
		isi_cfm_mapinfo_impl::iterator itr = impl.find(CHUNKSIZE(i), &error);

		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

		fail_if(impl.find(CHUNKSIZE(i), &error) == impl.end(),
		    "failed to find offset %ld", CHUNKSIZE(i));
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

		fail_if(impl.find(CHUNKSIZE(i) - 10, &error) != impl.end(),
		    "not supposed to find offset %ld", CHUNKSIZE(i) - 10);
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

		isi_error_free(error);
		error = NULL;

		fail_if(impl.find(CHUNKSIZE(i) + 10, &error) != impl.end(),
		    "not supposed to find offset %ld", CHUNKSIZE(i) + 10);
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

		isi_error_free(error);
		error = NULL;
	}

	// iterate in-store entries
	idx = 0;

	for (isi_cfm_mapinfo_impl::iterator itr = impl.begin(&error);
	    itr != impl.end(); itr.next(&error), ++idx) {
		fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));
		fail_if(itr->first != (off_t)CHUNKSIZE(idx),
		    "offset not match: %ld vs %ld",
		    itr->first, CHUNKSIZE(idx));

		fail_if(itr->second.get_offset() != (off_t)CHUNKSIZE(idx),
		    "offset not match: %ld vs %ld",
		    itr->second.get_offset(), CHUNKSIZE(idx));
	}
	fail_if(error, "Unexpected error: %{}", isi_error_fmt(error));

	fail_if(idx != mapinfo.get_overflow_threshold() + EXTRA,
	    "iterate through %d entries, %d expected",
	    idx, mapinfo.get_overflow_threshold() + EXTRA);

	// check copy
	{
		isi_cfm_mapinfo mapinfo_backup(ISI_CFM_MAP_TYPE_OBJECT);
		isi_cfm_mapinfo_impl impl_backup(&mapinfo_backup);

		impl_backup.copy_from(impl, &error);

		fail_if(error, "failed to copy the entries: %{}",
		    isi_error_fmt(error));

		fail_if(!impl_backup.equals(impl, &error),
		    "the copied map is supposed to be equal to the source");

		fail_if(error, "failed to compare the entris: %{}",
		    isi_error_fmt(error));

		impl_backup.remove(&error);
		fail_if(error, "failed to clear mapinfo: %{}",
		    isi_error_fmt(error));
	}

	// remove sbt store
	impl.remove(&error);
	fail_if(error, "failed to clear mapinfo: %{}", isi_error_fmt(error));
}

TEST(test_cbm_id)
{
	const char *guid[] = {
		"00259029b58630f24b53890cf846f74f38d5",
		"00259029b586b299425392026638f891522c",
	};

	for (unsigned i = 0; i < sizeof(guid) / sizeof(guid[0]); i++) {
		isi_cbm_id acct(1234, guid[i]);
		isi_cbm_id acct_back(0, "");
		char pack_str[22 + 1];
		char *ppack_str;

		bzero(pack_str, sizeof(pack_str));

		ppack_str = pack_str;
		acct.pack(&ppack_str);

		ppack_str = pack_str;
		acct_back.unpack(&ppack_str);

		fail_if(acct != acct_back,
		    "account not packed/unpacked correctely");
	}
}

TEST(test_mapinfo_size)
{
	isi_cfm_mapinfo mapinfo(ISI_CFM_MAP_TYPE_OBJECT);
	printf("mapinfo size: %d\n", isi_cfm_mapinfo::get_upbound_pack_size());
	printf("mapentry size: %d\n", mapinfo.get_mapentry_pack_size(true));
	printf("mapentry threshold: %d\n", mapinfo.get_overflow_threshold());
}

