#include <check.h>
#include <stdio.h>
#include "isirep.h"
#include "msg_internal.h"

SUITE_DEFINE_FOR_FILE(isi_migrate_msg);

static unsigned char *msg_buf_padded;
static unsigned char *msg_buf;


struct msg_info_list_entry
{
	int id;
	char *name;
	char *serial;
};

/*
 * List of all messages in OneFS 6.5 or earlier that the sworker
 * needs to support (either for receiving or sending) for compatability
 * with older releases.  IE, these are messages whose contents CANNOT
 * change.
 *
 * Commented out messages are messages that are NOT required for
 * compatibility.
 *
 * THESE STRINGS SHOULD NOT BE CHANGED. 
 */
struct msg_info_list_entry all_6_5_external_msgs[] =
{
	{0, "REP_MSG", "uu"},
	{1, "NOOP_MSG", ""},
//	{2, "CONN_MSG", ""},
//	{3, "DISCON_MSG", ""},
	{4, "ERROR_MSG", "uus"},
	{5, "GROUP_MSG", "u"}, //Not sure about this one???
	{6, "OLD_CLUSTER_MSG", "sb"},
	{7, "KILL_MSG", "ss"},//Not sure about this one???
	{8, "AUTH_MSG", "us"},
	{12, "OLD_DATA_MSG", "d"},
	{13, "DONE_MSG", "u"},
	{14, "OLD_WORK_INIT_MSG", "ssssssuuussuu"},
//	{15, "WORK_REQ_MSG", "uuuuUuuu"},	// p->c
	{23, "ADS_MSG", "u"},
	{25, "OLD_DIR_MSG3", "suuuuuusbu"},
	{28, "SWORKER_TW_STAT_MSG", "uuuuuuuuuuuuUUUU"},
//	{29, "JOBCTL_MSG", "u"},		// c->pw
	{30, "LIST_MSG", "ud"},
	{31, "SNAP_MSG", "sssu"},
	{32, "SNAP_RESP_MSG", "us"},
	{35, "OLD_FILE_MSG4", "uUuuuususuUUU"},
	{36, "OLD_SYMLINK_MSG4", "uuuuussuuuUUU"},
	{37, "OLD_ACK_MSG2", "suUUUuuuuU"},
	{39, "POSITION_MSG", "ussuUsbU"},
	{41, "DIR_MSG4", "suuuuuusbuUu"},
	{42, "ACK_DIR_MSG", "sbu"},
	{43, "SHORT_HASH_RESP_MSG", "Uu"},
	{44, "FULL_HASH_DIR_REQ_MSG", "sbU"},
	{45, "FULL_HASH_FILE_REQ_MSG", "suUUU"},
	{46, "FULL_HASH_FILE_RESP_MSG", "suUUUUsuuu"},
	{47, "HASH_STOP_MSG", "usuUU"},
	{48, "HASH_QUIT_MSG", ""},
	{49, "HASH_ERROR_MSG", "u"},
//	{50, "WORK_RESP_MSG3", "sUUUbbu"},	// p->c		
//	{51, "SPLIT_REQ", ""},			// c->pw
//	{52, "SPLIT_RESP", "usUUUbbu"},		// p->c
	{53, "FILE_DATA_MSG", "UUsuUuuub"},
	{54, "OLD_FILE_BEGIN_MSG", "UUUUuuuuuuusususuub"},
	{55, "FILE_DONE_MSG", "uUUsuuus"},
	{56, "OLD_WORK_INIT_MSG4", "sssssssUuuussuussuusuUusuu"},
	{57, "ACK_MSG3", "UUUsu"},
	{58, "SIQ_ERROR_MSG", "ussuusuUsUb"},
	{60, "OLD_CLUSTER_MSG2", "ssUb"},
//	{61, "BANDWIDTH_INIT_MSG", "suu"},	// c->bw
//	{62, "THROTTLE_INIT_MSG", "suu"},	// c->bw
//	{63, "BANDWIDTH_STAT_MSG", "su"},	// c->bw
//	{64, "THROTTLE_STAT_MSG", "su"},	// c->bw
//	{65, "BANDWIDTH_MSG", "u"},		// p->c
//	{66, "THROTTLE_MSG", "u"},		// p->c
	{67, "OLD_TARGET_INIT_MSG", "sssUsssssuuu"},
	{68, "TARGET_CANCEL_MSG", "s"},
	{69, "GENERIC_ACK_MSG", "sus"},
	{70, "CLEANUP_SYNC_RUN_MSG", "s"},
	{71, "TARGET_MONITOR_SHUTDOWN_MSG", "s"},
	{72, "JOB_STATUS_MSG", "u"},
	{73, "OLD_TARGET_RESP_MSG", "sssb"},
	{74, "TARGET_POLICY_DELETE_MSG", "s"},
	{75, "OLD_WORK_INIT_MSG", "sssssssUuussuuuusuUusuuUU"},
	{80, "DELETE_LIN_MSG", "U"},
	{81, "LINK_MSG", "UUsuusuu"},
	{82, "UNLINK_MSG", "UUsuu"},
	{83, "LIN_UPDATE_MSG", "uUuuuuUUUUUsusuuuUsu"},
	{84, "LIN_COMMIT_MSG", "Uus"},
	{85, "CLEANUP_TARGET_TMP_FILES_MSG", "ss"},
	{86, "LIST_MSG2", "uuuud"},
//	{87, "PWORKER_TW_STAT_MSG", ""},	// p->c
	{88, "OLD_TARGET_RESP_MSG2", "sssbUu"},
	{89, "WORK_INIT_MSG", "sssssssUuussuuuusuUusuuUUUu"},
	{90, "LIN_MAP_MSG", "suUUU"},
	{91, "FILE_BEGIN_MSG", "UUUUuuuuuUUUUsususuubUu"},
	{92, "DIR_UPGRADE_MSG", "UU"},
	{93, "UPGRADE_COMPLETE_MSG", "Uu"},
	{94, "LIN_ACK_MSG", "UUuuuu"},
	{95, "TARGET_POL_STF_DOWNGRADE_MSG", "s"},
//	{96, "PWORKER_STF_STAT_MSG", "uuuuuuuuuuuuuuuuuuuuUUUUUUU"}, // p->c
	{97, "SWORKER_STF_STAT_MSG", "uuuuuuu"},
	{98, "USER_ATTR_MSG", "uuubb"}
};


#define PAD_CHAR (0xc1)
#define EDGE_PAD (64)
#define KNOWN_EMPTY_FDM_SIZE 56

/**
 * Write a known byte to the serialization buffer.
 */
static void
helper_clear_msg_buf(void)
{
	fail_unless(msg_buf_padded != NULL);
	memset(msg_buf_padded, PAD_CHAR, BUFSIZE + 2 * EDGE_PAD);
}

/**
 * Allocate serialization buffer with edge padding.
 */
static void
helper_alloc_msg_buf(void)
{
	fail_unless(msg_buf_padded == NULL);
	msg_buf_padded = malloc(BUFSIZE + 2 * EDGE_PAD);
	msg_buf = msg_buf_padded + EDGE_PAD;
	helper_clear_msg_buf();
}

/**
 * Free the serialization buffer.
 */
static void
helper_free_msg_buf(void)
{
	fail_unless(msg_buf_padded != NULL);
	free(msg_buf_padded);
	msg_buf = NULL;
	msg_buf_padded = NULL;
}

/**
 * Verify parts of the serialization buffer before and
 * after the message still contain the known byte.
 */
static void
helper_validate_msg_bounds(int msg_size)
{
	int i;
	fail_unless(msg_size <= BUFSIZE);
	fail_unless(msg_size >=0);
	fail_unless(msg_buf_padded != NULL);

	/* Check for modification to beginning packing */
	for (i = 0; i < EDGE_PAD; i++)
		fail_unless(msg_buf_padded[i] == PAD_CHAR);

	/* Check for modification past end of message(but still in BUFSIZE) */
	for (i = (EDGE_PAD + msg_size); i < ((BUFSIZE - msg_size) + EDGE_PAD); i++) {
		if (msg_buf_padded[i] != PAD_CHAR)
			fail_unless(msg_buf_padded[i] == PAD_CHAR);
	}

	/* Check for modifications past BUFSIZE)*/
	for (i = EDGE_PAD + BUFSIZE; i < (2*EDGE_PAD + BUFSIZE); i++)
		fail_unless(msg_buf_padded[i] == PAD_CHAR);
}

static void
isi_error_free_and_null(struct isi_error **error) {
	isi_error_free(*error);
	*error = NULL;
}

/*********************************************************************
 * Tests of the FILE_DATA_MSG
 ********************************************************************/

/**
 * Test that FILE_DATA_MSG size is the known size for an empty message.
 */
TEST(test_FDM_empty_known_size, .attrs="overnight")
{
	struct generic_msg msg = {};
	int ret;
	char *buf = malloc(MAX_MSG_FDM_DATA+10);
	memset(buf, 0x2, MAX_MSG_FDM_DATA+10);
	
	helper_alloc_msg_buf();
	

	msg.head.type = FILE_DATA_MSG;
	msg.body.file_data.data = buf;
	msg.body.file_data.data_type = MSG_DTYPE_DATA;
	msg.body.file_data.data_size = 0;
	msg.body.file_data.offset = 200;
	msg.body.file_data.checksum = 0;

	ret = generic_msg_pack(&msg, msg_buf, BUFSIZE);
	fail_unless(ret == KNOWN_EMPTY_FDM_SIZE);
	helper_validate_msg_bounds(ret);
	helper_free_msg_buf();
	free(buf);
}


/*
 * Test an intermediate size message.
 */
TEST(test_FDM_normal_message, .attrs="overnight")
{
	struct generic_msg msg = {};
	int ret;
	char *buf = malloc(MAX_MSG_FDM_DATA+10);
	memset(buf, 0x2, MAX_MSG_FDM_DATA+10);
	
	helper_alloc_msg_buf();
	

	msg.head.type = FILE_DATA_MSG;
	msg.body.file_data.data = buf;
	msg.body.file_data.data_type = MSG_DTYPE_DATA;
	msg.body.file_data.data_size = 100;
	msg.body.file_data.offset = 200;
	msg.body.file_data.checksum = 0;

	ret = generic_msg_pack(&msg, msg_buf, BUFSIZE);
	fail_unless(ret > 0);
	helper_validate_msg_bounds(ret);
	helper_free_msg_buf();
	free(buf);
}

/*
 * Test that a FILE_DATA_MESSAGE of maximum size works.
 */
TEST(test_FDM_max_size, .attrs="overnight")
{
	struct generic_msg msg = {};
	int ret;
	char *buf = malloc(MAX_MSG_FDM_DATA+10);
	memset(buf, 0x2, MAX_MSG_FDM_DATA+10);
	char *filename = malloc(MAXNAMELEN+1);
	memset(filename, 0, MAXNAMELEN+1);
	memset(filename, 0x20, MAXNAMELEN);
	
	helper_alloc_msg_buf();

	msg.head.type = FILE_DATA_MSG;
	msg.body.file_data.fname = filename;
	msg.body.file_data.data = buf;
	msg.body.file_data.data_type = MSG_DTYPE_DATA;
	msg.body.file_data.data_size = MAX_MSG_FDM_DATA;
	msg.body.file_data.offset = 200;
	msg.body.file_data.checksum = 0;

	ret = generic_msg_pack(&msg, msg_buf, BUFSIZE);
	fail_unless(ret > 0);
	helper_validate_msg_bounds(ret);
	helper_free_msg_buf();
	free(buf);
	free(filename);
}

#if 0
/*
 * Invalid Test case for now since we have ASSERT for this test case.
 * Test that a FILE_DATA_MESSAGE just beyond maximum size fails.
 */
TEST(test_FDM_too_big_msg)
{
	struct generic_msg msg = {};
	int ret;
	char *buf = malloc(MAX_MSG_FDM_DATA + 10);
	memset(buf, 0x2, MAX_MSG_FDM_DATA + 10);
	char *filename = malloc(MAXNAMELEN+1);
	memset(filename, 0, MAXNAMELEN+1);
	memset(filename, 0x20, MAXNAMELEN);
	
	helper_alloc_msg_buf();

	msg.head.type = FILE_DATA_MSG;
	msg.body.file_data.fname = filename;
	msg.body.file_data.data = buf;
	msg.body.file_data.data_type = MSG_DTYPE_DATA;
	msg.body.file_data.data_size = MAX_MSG_FDM_DATA + 1;
	msg.body.file_data.offset = 200;
	msg.body.file_data.checksum = 0;

	ret = generic_msg_pack(&msg, msg_buf, BUFSIZE);
	fail_unless(ret < 0);
	helper_validate_msg_bounds(0);
	helper_free_msg_buf();
	free(buf);
	free(filename);
}
#endif

/*
 * Test that growing a file data makes the message grow
 * by the same number of bytes.
 */
TEST(test_FDM_data_growth, .attrs="overnight")
{
	struct generic_msg msg = {};
	char *buf = malloc(MAX_MSG_FDM_DATA + 10);
	memset(buf, 0x2, MAX_MSG_FDM_DATA + 10);
	int size_1;
	int size_2;
	helper_alloc_msg_buf();

	msg.head.type = FILE_DATA_MSG;
	msg.body.file_data.data = buf;
	msg.body.file_data.data_type = MSG_DTYPE_DATA;
	msg.body.file_data.data_size = 100;
	msg.body.file_data.offset = 200;
	msg.body.file_data.checksum = 0;

	/* Create the first message */
	size_1 = generic_msg_pack(&msg, msg_buf, BUFSIZE);
	fail_unless(size_1 > 0);
	helper_validate_msg_bounds(size_1);
	helper_clear_msg_buf();

	/* Create a second message 17 bytes longer */
	msg.body.file_data.data_size += 17;
	size_2 = generic_msg_pack(&msg, msg_buf, BUFSIZE);
	fail_unless(size_2 > 0);
	helper_validate_msg_bounds(size_2);

	/* Second message must be bigger by exactly 17 */
	fail_unless(size_1 < size_2);
	fail_unless((size_2 - size_1) == 17);

	helper_free_msg_buf();
	free(buf);
}

TEST(test_external_6_5_messages_unchanged, .attrs="overnight")
{
	int i;
	int num_msg;
	struct mesgtab *mtab;
	char *mtab_pack;
	num_msg = sizeof(all_6_5_external_msgs)/
	    sizeof(struct msg_info_list_entry);

	for (i = 0; i < num_msg; i++) {
		mtab = msg_get_mesg_tab(all_6_5_external_msgs[i].id);
		fail_unless(mtab != NULL, "Missing message %d %s",
		    all_6_5_external_msgs[i].id,
		    all_6_5_external_msgs[i].name);
		mtab_pack = mtab->pack;
		if (!mtab_pack)
			mtab_pack = "";
		fail_unless(0 == strcmp(mtab_pack,
		    all_6_5_external_msgs[i].serial),
		    "Mismatched packing message %d %d %s %s (%s != %s)", i,
		    all_6_5_external_msgs[i].id,
		    all_6_5_external_msgs[i].name, mtab->name, mtab_pack,
		    all_6_5_external_msgs[i].serial);
	}
}

TEST(test_unpack_ript_msg)
{
	struct generic_msg m = {};
	// A nearly complete message consisting of the following fields.
	static const size_t BUFLEN = 98;
	char buf[] = {
	    // field, 1 ver 1, uint8
	    0x00, 0x01, SIQFT_UINT8, 0x01, 0x7F,
	    // field 2, ver 1, int8
	    0x00, 0x02, SIQFT_INT8, 0x01, 0xBB,
	    // field 2, ver 2, 5 byte string
	    0x00, 0x02, SIQFT_STRING, 0x02, 0x00, 0x00, 0x00, 0x05, 'T', 'e',
	    's', 't', 0x00,
	    // field 3, ver 1, int32
	    0x00, 0x03, SIQFT_INT32, 0x01, 0xFF, 0x67, 0x69, 0x81,
	    // field 4, ver 1, int16
	    0x00, 0x04, SIQFT_INT16, 0x01, 0x7F, 0xFF,
	    // field 4, ver 2, int64
	    0x00, 0x04, SIQFT_INT64, 0x02, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB,
	    0xCD, 0xEF,
	    // field 5, ver 1, 3 byte bytestream
	    0x00, 0x05, SIQFT_BYTESTREAM, 0x01, 0x00, 0x00, 0x00, 0x03, 0xBE,
	    0xEF, 0x01,
	    // field 5, ver 2, uint16
	    0x00, 0x05, SIQFT_UINT16, 0x02, 0x04, 0x56,
	    // field 6, ver 1, uint32
	    0x00, 0x06, SIQFT_UINT32, 0x02, 0x12, 0x34, 0x56, 0x78,
	    // field 6, ver 2, uint64
	    0x00, 0x06, SIQFT_UINT64, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	    0xFF, 0xFF,
	    // field 7, ver 1, float64
	    0x00, 0x07, SIQFT_FLOAT64, 0x01, 0x44, 0x44, 0x44, 0x44, 0x44,
	    0x44, 0x44, 0x44 };

	char expect_bs[] = { 0xBE, 0xEF, 0x01 };

	int8_t i8val;
	uint8_t u8val;
	int16_t i16val;
	uint16_t u16val;
	int32_t i32val;
	uint32_t u32val;
	int64_t i64val;
	uint64_t u64val;
	double f64val;
	char *strval;
	uint8_t *bsval;
	uint32_t bslen;
	struct isi_error *error = NULL;

	fail_unless(unpack_ript_msg(&m.body.ript, buf, BUFLEN) == BUFLEN);

	ript_msg_get_field_int8(&m.body.ript, 0x0001, 1, &i8val, &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, EINVAL));
	isi_error_free_and_null(&error);

	ript_msg_get_field_uint8(&m.body.ript, 0x0001, 1, &u8val, &error);
	fail_unless(error == NULL);
	fail_unless(u8val == 0x7f);

	ript_msg_get_field_uint8(&m.body.ript, 0x0001, 2, &u8val, &error);
	fail_unless(error != NULL);
	fail_unless(isi_system_error_is_a(error, ENOENT));
	isi_error_free_and_null(&error);

	ript_msg_get_field_int8(&m.body.ript, 0x0002, 1, &i8val, &error);
	fail_unless(error == NULL);
	fail_unless(i8val == (int8_t)0xbb);

	ript_msg_get_field_str(&m.body.ript, 0x0002, 2, &strval, &error);
	fail_unless(error == NULL);
	fail_unless(!strcmp(strval, "Test"));

	ript_msg_get_field_int32(&m.body.ript, 0x0003, 1, &i32val, &error);
	fail_unless(error == NULL);
	fail_unless(i32val == -9999999);

	ript_msg_get_field_int16(&m.body.ript, 0x0004, 1, &i16val, &error);
	fail_unless(error == NULL);
	fail_unless(i16val == 32767);

	ript_msg_get_field_int64(&m.body.ript, 0x0004, 2, &i64val, &error);
	fail_unless(error == NULL);
	fail_unless(i64val = 0x1234567890abcdef);

	ript_msg_get_field_bytestream(&m.body.ript, 0x0005, 1, &bsval, &bslen,
	    &error);
	fail_unless(error == NULL);
	fail_unless(bslen == 3);
	fail_unless(!memcmp(bsval, expect_bs, bslen));

	ript_msg_get_field_uint16(&m.body.ript, 0x0005, 2, &u16val, &error);
	fail_unless(error == NULL);
	fail_unless(u16val == 0x0456);

	ript_msg_get_field_uint32(&m.body.ript, 0x0006, 2, &u32val, &error);
	fail_unless(error == NULL);
	fail_unless(u32val == 0x12345678);

	ript_msg_get_field_uint64(&m.body.ript, 0x0006, 1, &u64val, &error);
	fail_unless(error == NULL);
	fail_unless(u64val == 0xFFFFFFFFFFFFFFFF);

	ript_msg_get_field_float64(&m.body.ript, 0x0007, 1, &f64val, &error);
	fail_unless(error == NULL);
	fail_unless(*((uint64_t *)&f64val) == 0x4444444444444444);

	_free_ript_msg(&m.body.ript);
}

TEST(test_pack_ript_msg)
{
	static const size_t BUFLEN = 98;
	struct isi_error *error = NULL;

	struct generic_msg m = {};
	struct generic_msg u = {};

	int16_t i16val;
	int32_t i32val;
	int64_t i64val;
	char *strval;
	uint8_t *bsval;
	uint32_t bslen;

	char buf[1024];
	uint8_t bs[] = { 0xBE, 0xEF, 0x01 };
	uint64_t fval = 0xFFFFFFFFFFFFFFFF;

	ript_msg_init(&m.body.ript);

	fail_unless(!ript_msg_set_field_uint8(&m.body.ript, 0x0001, 1, 0x7F));
	fail_unless(!ript_msg_set_field_int8(&m.body.ript, 0x0002, 1, 0xBB));
	fail_unless(!ript_msg_set_field_int16(&m.body.ript, 0x0004, 1, 32767));
	fail_unless(!ript_msg_set_field_uint16(&m.body.ript, 0x0005, 2,
	    0x0456));
	fail_unless(!ript_msg_set_field_int32(&m.body.ript, 0x0003, 1,
	    -9999999));
	fail_unless(!ript_msg_set_field_uint32(&m.body.ript, 0x0006, 2,
	    0x12345678));
	fail_unless(!ript_msg_set_field_int64(&m.body.ript, 0x0004, 2,
	    0x1234567890ABCDEF));
	fail_unless(!ript_msg_set_field_uint64(&m.body.ript, 0x0006, 1,
	    0xFFFFFFFFFFFFFFFF));
	fail_unless(!ript_msg_set_field_float64(&m.body.ript, 0x0007, 1,
	    *(double *)&fval));
	fail_unless(!ript_msg_set_field_str(&m.body.ript, 0x0002, 2, "Test"));
	fail_unless(!ript_msg_set_field_bytestream(&m.body.ript, 0x0005, 1,
	    bs, 3));

	fail_unless(pack_ript_msg(&m.body.ript, buf) == BUFLEN);

	fail_unless(unpack_ript_msg(&u.body.ript, buf, BUFLEN) == BUFLEN);
	ript_msg_get_field_str(&u.body.ript, 0x0002, 2, &strval, &error);
	fail_unless(error == NULL);
	fail_unless(!strcmp(strval, "Test"));

	ript_msg_get_field_int32(&u.body.ript, 0x0003, 1, &i32val, &error);
	fail_unless(error == NULL);
	fail_unless(i32val == -9999999);

	ript_msg_get_field_int16(&u.body.ript, 0x0004, 1, &i16val, &error);
	fail_unless(error == NULL);
	fail_unless(i16val == 32767);

	ript_msg_get_field_int64(&u.body.ript, 0x0004, 2, &i64val, &error);
	fail_unless(error == NULL);
	fail_unless(i64val = 0x1234567890abcdef);

	ript_msg_get_field_bytestream(&u.body.ript, 0x0005, 1, &bsval, &bslen,
	    &error);
	fail_unless(error == NULL);
	fail_unless(bslen == 3);
	fail_unless(!memcmp(bsval, bs, bslen));

	_free_ript_msg(&m.body.ript);
	_free_ript_msg(&u.body.ript);

}
