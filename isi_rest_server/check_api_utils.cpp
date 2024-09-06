#include <stdlib.h>

#include "api_utils.h"

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_leak_primer.hpp>
#include <isi_util_cpp/check_helper.hpp>
#include <isi_util_cpp/scoped_clean.h>
#include <isi_util_cpp/array_count.h>

SUITE_DEFINE_FOR_FILE(api_utils);

namespace {

const char *const VALID_TRUES[] = {
	"true",
	"True",
	"yEs",
	"1",
};

const char *const VALID_FALSES[] = {
	"no",
	"No",
	"falsE",
	"0",
};

const char *const VALID_ANYS[] = {
	"any",
	"anY",
	"aLL"
};

struct foo
{
	char data[666];
};

void
foo_cleaner(foo *f)
{
	free(f);
}

struct bar
{
	// cppcheck-suppress memleak
	char *data;

	bar()
	{
	    data = new char[444];
	}
};

void
bar_cleaner(bar *b)
{
	delete[] b->data;
}

}

TEST(api_utils_json)
{
	Json::Value v;

	fail_unless(non_empty_or_null("").type() == Json::nullValue);

	fail_unless(non_empty_or_null("hi").type() == Json::stringValue);

	fail_unless(non_zero_or_null(0).type() == Json::nullValue);
	fail_unless(non_zero_or_null(0UL).type() == Json::nullValue);

	fail_unless(non_zero_or_null(1).type() == Json::intValue);
	fail_unless(non_zero_or_null(1UL).type() == Json::uintValue);
}

/* XXX these tests really belong in isi_util_cpp now */
TEST(api_utils_scoped_ptr)
{
	foo *f1 = (foo *)calloc(1, sizeof *f1);
	foo *f3 = (foo *)calloc(1, sizeof *f3);

	scoped_ptr_clean<foo> f1_cleaner(f1, free);
	scoped_ptr_clean<foo> f3_cleaner(f3, foo_cleaner);

	bar b;

	scoped_clean<bar> b_cleaner(b, bar_cleaner);
}

TEST(api_utils_lexical_cast, .mem_check = CK_MEM_DISABLE)
{
	fail_unless(lexical_cast<int>("20") == 20);
	fail_unless(lexical_cast<int>("-20") == -20);
	fail_unless(lexical_cast<int>("0") == 0);
	fail_unless(lexical_cast<bool>("0") == false);
	fail_unless(lexical_cast<bool>("1") == true);


	fail_unless(lexical_cast<int>(" 0") == 0);

	fail_unless_throws_any(lexical_cast<int>("0.0"));
	fail_unless_throws_any(lexical_cast<int>("b"));
	fail_unless_throws_any(lexical_cast<int>("b0"));
	fail_unless_throws_any(lexical_cast<int>("0b"));
	fail_unless_throws_any(lexical_cast<int>("0 "));
}

TEST(api_utils_str_to_bool, .mem_check = CK_MEM_DISABLE)
{
	for (unsigned i = 0; i < isi::util::array_count(VALID_TRUES); ++i)
		fail_unless(str_to_bool(VALID_TRUES[i]) == true);

	for (unsigned i = 0; i < isi::util::array_count(VALID_FALSES); ++i)
		fail_unless(str_to_bool(VALID_FALSES[i]) == false);

	fail_unless_throws_any(str_to_bool("Foobar"));
}

TEST(api_utils_str_to_tribool, .mem_check = CK_MEM_DISABLE)
{
	for (unsigned i = 0; i < isi::util::array_count(VALID_TRUES); ++i)
		fail_unless(str_to_tribool(VALID_TRUES[i]) == tribool::TB_TRUE);

	for (unsigned i = 0; i < isi::util::array_count(VALID_FALSES); ++i)
		fail_unless(str_to_tribool(VALID_FALSES[i]) == tribool::TB_FALSE);

	for (unsigned i = 0; i < isi::util::array_count(VALID_ANYS); ++i)
		fail_unless(str_to_tribool(VALID_ANYS[i]) == tribool::TB_ANY);

	fail_unless_throws_any(str_to_tribool("Foobar"));
}

TEST(api_utils_find_args, .mem_check = CK_MEM_DISABLE)
{
	using std::pair;
	using std::string;

	/* test resolution for different types for automatic conversion */
	char     char_v;
	int      int_v;
	unsigned unsigned_v;
	bool     bool_v;
	string   string_v;

	request::arg_map args;

	args.insert(pair<string, string>("char_v",     "v"));
	args.insert(pair<string, string>("int_v",      "-999"));
	args.insert(pair<string, string>("unsigned_v", "2323"));
	args.insert(pair<string, string>("bool_v",     "true"));
	args.insert(pair<string, string>("string_v",   "hi"));

	fail_unless(find_args(char_v, args, "char_v"));
	fail_unless(char_v == 'v');
	fail_unless(find_args(int_v, args, "int_v"));
	fail_unless(int_v == -999);
	fail_unless(find_args(unsigned_v, args, "unsigned_v"));
	fail_unless(unsigned_v == 2323);
	fail_unless(find_args(bool_v, args, "bool_v"));
	fail_unless(bool_v == true);
	fail_unless(find_args(string_v, args, "string_v"));
	fail_unless(string_v == "hi");
}
