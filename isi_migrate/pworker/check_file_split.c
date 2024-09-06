#include "pworker.h"

#include <check.h>
#include <isi_util/check_helper.h>

TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);
TEST_FIXTURE(test_setup);
TEST_FIXTURE(test_teardown);

#define FSPLIT_TEST_TIMEOUT 240

#define MB 1024*1024
#define GB 1024*1024*1024
#define TB 1024*1024*1024*1024
#define VALID_DUMMY_FD 10
#define VALID_DUMMY_LIN 4294967296

SUITE_DEFINE_FOR_FILE(file_split, .suite_setup = suite_setup,
    .suite_teardown = suite_teardown, .test_setup = test_setup,
    .test_teardown = test_teardown, .timeout = FSPLIT_TEST_TIMEOUT);

struct migr_file_state file;
struct migr_pworker_ctx pw_ctx;
struct work_restart_state curr_work;
struct work_restart_state new_work;
bool success = false;

/* built in pow doesnt have one accepting ints and its a pain */
static int
mypow(int base, int exp)
{
	int result, i;

	if (exp == 0)
		return 1;

	result = base;
	for (i = 1; i < exp; i++)
		result *= base;

	return result;
}

/* The splitting of a file can be viewed as a perfect binary tree,
 * where each split produces two work items with an equal range.
 * When file splitting, the file range is divided in half unless it is
 * below the file splitting threshold.
 * Consider the picture where each node value is the size of the file
 * range in the file split work item:
 * 				
 * 				60
 *			      /    \
 *			     30    30
 *			    /  \  /  \
 *			   15  1515  15
 *
 * with a starting file range of 60 mb, and a split threshold >= 20mb,
 * the work item will be split three times. Each internal node
 * represents a split. Therefore, the number of internal nodes is how
 * many splits we should expect.
 * Total number of nodes N = 2^(d+1) - 1 (where d = tree depth).
 * Number of leaf nodes  L = 2^d
 * ==> num_internal_nodes = N - L == num_splits. 
 */

static inline int
get_num_splits_from_depth(int depth)
{
	return ((mypow(2, depth + 1) - 1) - mypow(2, depth));
}

static int
get_num_splits_expected(uint64_t file_size_mb)
{
	int depth = 0, num_splits = 0;
	
	while (file_size_mb >= MIN_FILE_SPLIT_SIZE) {
		depth++;
		file_size_mb /= 2;
	}
	
	num_splits = get_num_splits_from_depth(depth);
	return num_splits;
}

static void
init_file_attr(int fd, enum file_type type, off_t cur_offset, off_t size)
{
	file.fd = fd;
	file.type = type;
	file.cur_offset = cur_offset;
	file.st.st_size = size;
	file.st.st_ino = VALID_DUMMY_LIN;
}

static void
init_curr_work_range(int f_low, int f_high)
{
	pw_ctx.work->f_low = f_low;
	pw_ctx.work->f_high = f_high;

	/* The work item is a result of a file split if it has f_high set,
	 * so give it a valid fsplit_lin. 
	 * Asserts are in the pworker logic to enforce this assumption */
	if (f_high)
		pw_ctx.work->fsplit_lin = VALID_DUMMY_LIN;
}



TEST_FIXTURE(suite_setup)
{
}

TEST_FIXTURE(suite_teardown)
{
}

TEST_FIXTURE(test_setup)
{
	memset(&file, 0, sizeof(struct migr_file_state));
	memset(&pw_ctx, 0, sizeof(struct migr_pworker_ctx));
	memset(&curr_work, 0, sizeof(struct work_restart_state));
	memset(&new_work, 0, sizeof(struct work_restart_state));
	pw_ctx.work = &curr_work;
	success = false;
}

TEST_FIXTURE(test_teardown)
{
}

TEST(bad_file_descriptor)
{
	init_file_attr(-1, SIQ_FT_REG, 0, MIN_FILE_SPLIT_SIZE);

	success = init_file_split_work(&pw_ctx, &new_work, &file);

	fail_unless(success == false);
}

TEST(invalid_file_type_sym)
{
	init_file_attr(VALID_DUMMY_FD, SIQ_FT_SYM, 0, MIN_FILE_SPLIT_SIZE);

	success = init_file_split_work(&pw_ctx, &new_work, &file);

	fail_unless(success == false);
}

TEST(invalid_file_type_char)
{
	init_file_attr(VALID_DUMMY_FD, SIQ_FT_CHAR, 0, MIN_FILE_SPLIT_SIZE);

	success = init_file_split_work(&pw_ctx, &new_work, &file);

	fail_unless(success == false);
}

TEST(invalid_file_type_block)
{
	init_file_attr(VALID_DUMMY_FD, SIQ_FT_BLOCK, 0, MIN_FILE_SPLIT_SIZE);

	success = init_file_split_work(&pw_ctx, &new_work, &file);

	fail_unless(success == false);
}

TEST(invalid_file_type_sock)
{
	init_file_attr(VALID_DUMMY_FD, SIQ_FT_SOCK, 0, MIN_FILE_SPLIT_SIZE);

	success = init_file_split_work(&pw_ctx, &new_work, &file);

	fail_unless(success == false);
}

TEST(invalid_file_type_dir)
{
	init_file_attr(VALID_DUMMY_FD, SIQ_FT_DIR, 0, MIN_FILE_SPLIT_SIZE);

	success = init_file_split_work(&pw_ctx, &new_work, &file);

	fail_unless(success == false);
}

TEST(invalid_file_type_fifo)
{
	init_file_attr(VALID_DUMMY_FD, SIQ_FT_FIFO, 0, MIN_FILE_SPLIT_SIZE);

	success = init_file_split_work(&pw_ctx, &new_work, &file);

	fail_unless(success == false);
}

TEST(invalid_file_type_unknown)
{
	init_file_attr(VALID_DUMMY_FD, SIQ_FT_UNKNOWN, 0, MIN_FILE_SPLIT_SIZE);

	success = init_file_split_work(&pw_ctx, &new_work, &file);

	fail_unless(success == false);
}

/* covers MIN_FILE_SPLIT_SIZE - 1 boundary */
TEST(file_too_small)
{
	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, MIN_FILE_SPLIT_SIZE-1);
	/* This work item has not been split before */
	init_curr_work_range(0, 0);

	success = init_file_split_work(&pw_ctx, &new_work, &file);

	fail_unless(success == false);
}

TEST(work_range_too_small_no_progress)
{
	/* MIN_FILE_SPLIT_SIZE file, no progress */
	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, MIN_FILE_SPLIT_SIZE);

	/* This work item has been split before, has half the file, no prog */
	init_curr_work_range(0, MIN_FILE_SPLIT_SIZE/2);

	success = init_file_split_work(&pw_ctx, &new_work, &file);

	fail_unless(success == false);

}

TEST(work_range_too_small_some_progress)
{
	/* MIN_FILE_SPLIT_SIZE file, progress has been made on the file,
	   leaving the range between progress and EOF too small to split */
	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 100, MIN_FILE_SPLIT_SIZE);

	/* This work item has not been split before */
	init_curr_work_range(0, 0);

	success = init_file_split_work(&pw_ctx, &new_work, &file);

	fail_unless(success == false);

}

/* tests that values are correct after the first file split on a work item.
 * Covers MIN_FILE_SPLIT_SIZE boundary */
TEST(first_file_split)
{
	uint64_t file_low, file_mid, file_high;
	file_low = 0;
	file_mid = MIN_FILE_SPLIT_SIZE/2;
	file_high = MIN_FILE_SPLIT_SIZE;

	/* MIN_FILE_SPLIT_SIZE file, no progress */
	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, MIN_FILE_SPLIT_SIZE);

	/* This work item has not been split before */
	init_curr_work_range(0, 0);

	/* split once */
	success = init_file_split_work(&pw_ctx, &new_work, &file);
	fail_unless(success == true);
	fail_unless(pw_ctx.work->f_low == 0);
	fail_unless(pw_ctx.work->f_high == file_mid);
	fail_unless(new_work.f_low == file_mid);
	fail_unless(new_work.f_high == file_high);
}

/* tests that values are correct after the second file split on a work item */
TEST(second_file_split)
{
	uint64_t file_low, file_mid, file_high;
	file_low = 0;
	file_mid = MIN_FILE_SPLIT_SIZE/2;
	file_high = MIN_FILE_SPLIT_SIZE;

	/* MIN_FILE_SPLIT_SIZE*2 file, no progress */
	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, MIN_FILE_SPLIT_SIZE*2);

	/* This work item has not been split before */
	init_curr_work_range(0, 0);

	/* Split twice */
	success = init_file_split_work(&pw_ctx, &new_work, &file);
	fail_unless(success == true);
	memset(&new_work, 0, sizeof(struct work_restart_state));
	success = init_file_split_work(&pw_ctx, &new_work, &file);
	fail_unless(success == true);
	fail_unless(pw_ctx.work->f_low == 0);
	fail_unless(pw_ctx.work->f_high == file_mid);
	fail_unless(new_work.f_low == file_mid);
	fail_unless(new_work.f_high == file_high);

}

TEST(test_file_size_threshold)
{
	uint64_t file_size = MIN_FILE_SPLIT_SIZE;
	bool split_res = true;
	int split_tree_depth = 0, num_splits = 0, expected_splits = 0;

	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, file_size);
	init_curr_work_range(0,0);

	split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	while (split_res){ 
		split_tree_depth++;
		split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	}
	num_splits = get_num_splits_from_depth(split_tree_depth);
	expected_splits = get_num_splits_expected(file_size);	
	fail_unless(num_splits == expected_splits);
}

TEST(test_file_size_threshold_minus_one)
{
	uint64_t file_size = MIN_FILE_SPLIT_SIZE - 1;
	bool split_res = true;
	int split_tree_depth = 0, num_splits = 0, expected_splits = 0;

	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, file_size);
	init_curr_work_range(0,0);

	split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	while (split_res){ 
		split_tree_depth++;
		split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	}
	num_splits = get_num_splits_from_depth(split_tree_depth);
	expected_splits = get_num_splits_expected(file_size);	
	fail_unless(num_splits == expected_splits);
}

TEST(test_file_size_threshold_plus_one)
{
	uint64_t file_size = MIN_FILE_SPLIT_SIZE + 1;
	bool split_res = true;
	int split_tree_depth = 0, num_splits = 0, expected_splits = 0;

	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, file_size);
	init_curr_work_range(0,0);

	split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	while (split_res){ 
		split_tree_depth++;
		split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	}
	num_splits = get_num_splits_from_depth(split_tree_depth);
	expected_splits = get_num_splits_expected(file_size);	
	fail_unless(num_splits == expected_splits);
}

TEST(test_file_size_40MB)
{
	uint64_t file_size = 40*MB;
	bool split_res = true;
	int split_tree_depth = 0, num_splits = 0, expected_splits = 0;

	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, file_size);
	init_curr_work_range(0,0);

	split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	while (split_res){ 
		split_tree_depth++;
		split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	}
	num_splits = get_num_splits_from_depth(split_tree_depth);
	expected_splits = get_num_splits_expected(file_size);	
	fail_unless(num_splits == expected_splits);
}

TEST(test_file_size_80MB)
{
	uint64_t file_size = 80*MB;
	bool split_res = true;
	int split_tree_depth = 0, num_splits = 0, expected_splits = 0;

	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, file_size);
	init_curr_work_range(0,0);

	split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	while (split_res){ 
		split_tree_depth++;
		split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	}
	num_splits = get_num_splits_from_depth(split_tree_depth);
	expected_splits = get_num_splits_expected(file_size);	
	fail_unless(num_splits == expected_splits);
}

TEST(test_file_size_100MB)
{
	uint64_t file_size = 100*MB;
	bool split_res = true;
	int split_tree_depth = 0, num_splits = 0, expected_splits = 0;

	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, file_size);
	init_curr_work_range(0,0);

	split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	while (split_res){ 
		split_tree_depth++;
		split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	}
	num_splits = get_num_splits_from_depth(split_tree_depth);
	expected_splits = get_num_splits_expected(file_size);	
	fail_unless(num_splits == expected_splits);
}

TEST(test_file_size_1GB)
{
	uint64_t file_size = GB;
	bool split_res = true;
	int split_tree_depth = 0, num_splits = 0, expected_splits = 0;

	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, file_size);
	init_curr_work_range(0,0);

	split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	while (split_res){ 
		split_tree_depth++;
		split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	}
	num_splits = get_num_splits_from_depth(split_tree_depth);
	expected_splits = get_num_splits_expected(file_size);	
	fail_unless(num_splits == expected_splits);
}

TEST(test_file_size_10GB)
{
	uint64_t file_size = (uint64_t)10*GB;
	bool split_res = true;
	int split_tree_depth = 0, num_splits = 0, expected_splits = 0;

	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, file_size);
	init_curr_work_range(0,0);

	split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	while (split_res){ 
		split_tree_depth++;
		split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	}
	num_splits = get_num_splits_from_depth(split_tree_depth);
	expected_splits = get_num_splits_expected(file_size);	
	fail_unless(num_splits == expected_splits);
}

TEST(test_file_size_100GB)
{
	uint64_t file_size = (uint64_t)100*GB;
	bool split_res = true;
	int split_tree_depth = 0, num_splits = 0, expected_splits = 0;

	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, file_size);
	init_curr_work_range(0,0);

	split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	while (split_res){ 
		split_tree_depth++;
		split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	}
	num_splits = get_num_splits_from_depth(split_tree_depth);
	expected_splits = get_num_splits_expected(file_size);	
	fail_unless(num_splits == expected_splits);
}

TEST(test_file_size_1TB)
{
	uint64_t file_size = (uint64_t)TB;
	bool split_res = true;
	int split_tree_depth = 0, num_splits = 0, expected_splits = 0;

	init_file_attr(VALID_DUMMY_FD, SIQ_FT_REG, 0, file_size);
	init_curr_work_range(0,0);

	split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	while (split_res){ 
		split_tree_depth++;
		split_res = init_file_split_work(&pw_ctx, &new_work, &file);
	}
	num_splits = get_num_splits_from_depth(split_tree_depth);
	expected_splits = get_num_splits_expected(file_size);	
	fail_unless(num_splits == expected_splits);
}
