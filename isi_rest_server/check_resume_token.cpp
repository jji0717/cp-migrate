#include "resume_token.h"

#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

SUITE_DEFINE_FOR_FILE(check_resume_token,
    .mem_check    = CK_MEM_DISABLE, /* XXX for now */
    .dmalloc_opts = NULL,
);

TEST(resume_token_invalid)
{
	std::string sval;
	int         ival;

	// invalid version
	{
		resume_output_token rot(2);
		fail_unless_throws_any(
		    resume_input_token rit(rot.str(), 1);
		    );
	}

	// string from nothing
	{
		resume_output_token rot(1);
		resume_input_token rit(rot.str(), 1);
		fail_unless_throws_any(
		    rit >> sval;
		    );
	}

	// int from nothing
	{
		resume_output_token rot(1);
		resume_input_token rit(rot.str(), 1);
		fail_unless_throws_any(
		    rit >> ival;
		    );
	}

	// exhausted strings
	{
		resume_output_token rot(1);
		rot << "hi";
		resume_input_token rit(rot.str(), 1);
		rit >> sval;
		fail_unless_throws_any(
		    rit >> sval;
		    );
	}

	// exhausted ints
	{
		resume_output_token rot(1);
		rot << 1234;
		resume_input_token rit(rot.str(), 1);
		rit >> ival;
		fail_unless_throws_any(
		    rit >> ival;
		    );
	}

	// int from string, various failing cases
	{
		resume_output_token rot(1);
		rot << "";
		resume_input_token rit(rot.str(), 1);
		fail_unless_throws_any(
		    rit >> ival;
		    );
	}
	{
		resume_output_token rot(1);
		rot << "hello";
		resume_input_token rit(rot.str(), 1);
		fail_unless_throws_any(
		    rit >> ival;
		    );
	}
	{
		resume_output_token rot(1);
		rot << "1234 ";
		resume_input_token rit(rot.str(), 1);
		fail_unless_throws_any(
		    rit >> ival;
		    );
	}
	{
		resume_output_token rot(1);
		rot << " 1234 ";
		resume_input_token rit(rot.str(), 1);
		fail_unless_throws_any(
		    rit >> ival;
		    );
	}
}

TEST(resume_token_valid)
{
	const std::string strs[] = {
		"", "a", "ab", "abc", "abcd", " hello world !#234 \t\n "
	};
	const unsigned num_strs = sizeof strs / sizeof strs[0];

	const int ints[] = {
		-404, -1, 0, 1, 2, 3, 9999, 10000
	};
	const unsigned num_ints = sizeof ints / sizeof ints[0];

	std::string sval;
	int         ival;

	// empty
	{
		resume_output_token rot(1);
		resume_input_token rit(rot.str(), 1);
	}

	// another version, empty
	{
		resume_output_token rot(999);
		resume_input_token rit(rot.str(), 999);
	}

	// single string
	for (unsigned i = 0; i < num_strs; ++i) {
		resume_output_token rot(1);
		rot << strs[i];
		resume_input_token rit(rot.str(), 1);
		rit >> sval;
		fail_unless(sval == strs[i]);
	}

	// string then string
	for (unsigned i = 0; i < num_strs; ++i) {
		for (unsigned j = 0; j < num_strs; ++j) {
			resume_output_token rot(1);
			rot << strs[i];
			rot << strs[j];
			resume_input_token rit(rot.str(), 1);
			rit >> sval;
			fail_unless(sval == strs[i]);
			rit >> sval;
			fail_unless(sval == strs[j]);
		}
	}
	
	// single int
	for (unsigned i = 0; i < num_ints; ++i) {
		resume_output_token rot(1);
		rot << ints[i];
		resume_input_token rit(rot.str(), 1);
		rit >> ival;
		fail_unless(ival == ints[i]);
	}

	// int then int
	for (unsigned i = 0; i < num_ints; ++i) {
		for (unsigned j = 0; j < num_ints; ++j) {
			resume_output_token rot(1);
			rot << ints[i];
			rot << ints[j];
			resume_input_token rit(rot.str(), 1);
			rit >> ival;
			fail_unless(ival == ints[i]);
			rit >> ival;
			fail_unless(ival == ints[j]);
		}
	}

	// int then string
	for (unsigned i = 0; i < num_ints; ++i) {
		for (unsigned j = 0; j < num_strs; ++j) {
			resume_output_token rot(1);
			rot << ints[i];
			rot << strs[j];
			resume_input_token rit(rot.str(), 1);
			rit >> ival;
			rit >> sval;
			fail_unless(ival == ints[i]);
			fail_unless(sval == strs[j]);
		}
	}

	// string then int
	for (unsigned i = 0; i < num_strs; ++i) {
		for (unsigned j = 0; j < num_ints; ++j) {
			resume_output_token rot(1);
			rot << strs[i];
			rot << ints[j];
			resume_input_token rit(rot.str(), 1);
			rit >> sval;
			rit >> ival;
			fail_unless(sval == strs[i]);
			fail_unless(ival == ints[j]);
		}
	}

	// all chars
	{
		std::string s1, s2;
		for (unsigned i = 0; i < 256; ++i)
			s1 += (char)i;
		resume_output_token rot(1);
		rot << s1;
		resume_input_token rit(rot.str(), 1);
		rit >> s2;
		fail_unless(s1 == s2);
	}
}

struct foo {
	int      i32;
	char     chr;
	uint64_t u64;
	short    i16;

	foo (int i = 0, char c = 0, uint64_t u = 0, short j = 0)
	    : i32(i), chr(c), u64(u), i16(j)
	{ }

	bool operator==(const foo &x) const
	{
		return i32 == x.i32
		    && chr == x.chr
		    && u64 == x.u64
		    && i16 == x.i16;
	}
};

TEST(resume_token_blobs)
{

	{ // single struct blob
		foo ifoo(9, 2, 21, 32), ofoo;
		resume_output_token rot(1);
		rot << resume_token_blob<foo>(ifoo);
		resume_input_token rit(rot.str(), 1);
		rit >> resume_token_blob<foo>(ofoo);
		fail_unless(ifoo == ofoo);
	}

	{ // double struct blob
		foo ifoo(9, 2, 21, 32), ofoo;
		resume_output_token rot(1);
		rot << resume_token_blob<foo>(ifoo);
		rot << resume_token_blob<foo>(ifoo);
		resume_input_token rit(rot.str(), 1);
		rit >> resume_token_blob<foo>(ofoo);
		rit >> resume_token_blob<foo>(ofoo);
		fail_unless(ifoo == ofoo);
	}

	{ // mixed with other stuff, add blob on simple type
		foo ifoo(9, 2, 21, 32), ofoo;
		int iint(21), oint;
		std::string istr("hithere"), ostr;
		uint64_t iu64(9999), ou64;
		resume_output_token rot(1);
		rot << iint;
		rot << resume_blob(ifoo);
		rot << istr;
		rot << resume_blob(iu64);
		resume_input_token rit(rot.str(), 1);
		rit >> oint;
		rit >> resume_blob(ofoo);
		rit >> ostr;
		rit >> resume_blob(ou64);
		fail_unless(ifoo == ofoo);
		fail_unless(iint == oint);
		fail_unless(iu64 == ou64);
		fail_unless(istr == ostr);
	}
}
