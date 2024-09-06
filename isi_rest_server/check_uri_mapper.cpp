#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "uri_mapper.h"

SUITE_DEFINE_FOR_FILE(check_uri_mapper,
    .mem_check    = CK_MEM_DISABLE, /* XXX for now */
    .dmalloc_opts = NULL,
);

namespace {

/* 
 * Test support functions, real handlers replaced by strings. Do not
 * mix with real handlers, call map_clean before map destructor
 * is called.
 */

/* add path with handler faked to be path */
void
map_add(uri_mapper &m, const char *path)
{
	m.add(path, NULL);
}

/* check uri against map with fake handlers, expect is exact rule path that
 * should match or NULL */
#define map_check(M, R, U, E) _map_check(M, R, U, E, __LINE__)
void
_map_check(uri_mapper &m, uri_tokens &r, const char *u, const char *e, int line)
{
	uri_mapper::pattern_handler *ph = m.resolve_pattern(u, r);

	const char *result = NULL;
	if (ph && ph->begin() != ph->end()) {
		result = ph->begin()->first.c_str();
	}

	if (e == NULL)
		_fail_unless(result == NULL, __FILE__, line,
		    "False match for %s", u);
	else if (result == NULL) {
		_fail_unless(false, __FILE__, line, "No match for %s", u);
		return;
	} else {
		_fail_unless(strcmp(result, e) == 0, __FILE__, line,
		    "Mismatch %s != %s", result, e);
	}
}

}

TEST(uri_mapper_basic)
{
	uri_mapper m(1);

	m.add("/z/bar");
	fail_unless(m.size() == 1);

	m.add("/z");
	m.add("/z/baz");
	fail_unless(m.size() == 3);

	m.add("/bar/<ID1>");
	m.add("/bar/<ID1>/stuff/<ID2>");
	fail_unless(m.size() == 5);

	m.add("/foo/<ID1>/stuff/<ID2>/crap");
	m.add("/foo/<ID1>/stuff/<ID2>/crap/p/<ID3*>");
	fail_unless(m.size() == 7);

	m.add("/baz");
	m.add("/baz/<ID1>");
	m.add("/baz/<ID1>/stuff");
	m.add("/baz/<ID1>/stuff/<ID2>");

	m.add("/a/<ID1>");
	m.add("/a/<ID1>/<ID2>");

	m.add("/b/<ID1>");
	m.add("/b/<ID1>/<ID2+>");
}

/* test various duplicates */
TEST(uri_mapper_dups)
{
	uri_mapper m(0);

	// exact dups
	m.add("/e1/bar");
	fail_unless_throws_any(m.add("/e1/bar"));
	m.add("/e1/bar/<ID1>");
	fail_unless_throws_any(m.add("/e1/bar/<ID1>"));
	m.add("/e1/bar/<ID1>/baz");
	fail_unless_throws_any(m.add("/e1/bar/<ID1>/baz"));
	m.add("/e1/bar/<ID1>/baz/<ID2>");
	fail_unless_throws_any(m.add("/e1/bar/<ID1>/baz/<ID2>"));
	m.add("/e1/bar/<ID1>/c/<ID2*>");
	fail_unless_throws_any(m.add("/e1/bar/<ID1>/c/<ID2*>"));

	// ambiguity dups staring with SINGLE
	m.add("/foo/<ID1>");
	fail_unless_throws_any(m.add("/foo/<ID1>"));
	fail_unless_throws_any(m.add("/foo/<ID2>"));
	fail_unless_throws_any(m.add("/foo/stuff"));
	fail_unless_throws_any(m.add("/foo/<ID2*>"));
	fail_unless_throws_any(m.add("/foo/<ID3+>"));

	// splat ambiguity dups
	m.add("/bar/<ID1*>");
	fail_unless_throws_any(m.add("/bar/<ID2*>"));
	fail_unless_throws_any(m.add("/bar/<ID3>"));
	fail_unless_throws_any(m.add("/bar/<ID4+>"));
	fail_unless_throws_any(m.add("/bar/stuff"));
	fail_unless_throws_any(m.add("/bar"));

	// plus ambiguity dups
	m.add("/boz/<ID1+>");
	fail_unless_throws_any(m.add("/boz/<ID2*>"));
	fail_unless_throws_any(m.add("/boz/<ID3>"));
	fail_unless_throws_any(m.add("/boz/<ID4+>"));
	fail_unless_throws_any(m.add("/boz/stuff"));
}

TEST(uri_mapper_invalid_zero)
{
	uri_mapper m(0);

	fail_unless_throws_any(m.add("//"));
	fail_unless_throws_any(m.add("/asdf//asdf"));
	fail_unless_throws_any(m.add("/<"));
	fail_unless_throws_any(m.add("/<>"));
	fail_unless_throws_any(m.add("/asdf/<ASDF>asdf"));
	fail_unless_throws_any(m.add("/asdf/<ASDF>/foo/<ASDF>"));
	fail_unless_throws_any(m.add("/asdf/<ASDF*>/foo/"));
	fail_unless_throws_any(m.add("/asdf/<ASDF+>/foo/"));
}

TEST(uri_mapper_invalid_one)
{
	uri_mapper m(1);

	fail_unless_throws_any(m.add(""));
	fail_unless_throws_any(m.add("/"));
	fail_unless_throws_any(m.add("/<ASDF>"));
	fail_unless_throws_any(m.add("/a/asdf//asdf"));
	fail_unless_throws_any(m.add("/a/<"));
	fail_unless_throws_any(m.add("/a/<>"));
	fail_unless_throws_any(m.add("/a//<ASDF>"));
	fail_unless_throws_any(m.add("/a/asdf/<ASDF>asdf"));
	fail_unless_throws_any(m.add("/a/asdf/<ASDF>/foo/<ASDF>"));
	fail_unless_throws_any(m.add("/a/asdf/<ASDF*>/foo/"));
	fail_unless_throws_any(m.add("/a/asdf/<ASDF+>/foo/"));
}

TEST(uri_mapper_matches)
{
	uri_mapper m(1);
	uri_tokens r;

	/* literal set */
	map_add(m, "/foo");
	map_add(m, "/foo/bar");
	map_add(m, "/foo/baz");

	/* t1 set */
	map_add(m, "/t1");
	map_add(m, "/t1/<ID1>");
	map_add(m, "/t1/<ID1>/foo");
	map_add(m, "/t1/<ID1>/foo/<ID2>");
	map_add(m, "/t1/<ID1>/bar");
	map_add(m, "/t1/<ID1>/bar/<ID2>");

	/* t2 set */
	map_add(m, "/t2");
	map_add(m, "/t2/<ID1>");
	map_add(m, "/t2/<ID1>/<ID2>");
	map_add(m, "/t2/<ID1>/<ID2>/<ID3>");
	map_add(m, "/t2/<ID1>/<ID2>/<ID3>/f");
	map_add(m, "/t2/<ID1>/<ID2>/<ID3>/f/<ID4>");

	/* t3 set - no intermediates */
	map_add(m, "/t3");
	map_add(m, "/t3/<ID1>/foo/<ID2>");

	/* greedy set */
	map_add(m, "/greedy");
	map_add(m, "/greedy/<ID1>");
	map_add(m, "/greedy/<ID1>/p/<ID2*>");

	/* invalid input paths */
	map_check(m, r, "",   NULL);
	map_check(m, r, "a",  NULL);
	map_check(m, r, "/",  NULL);
	map_check(m, r, "aa", NULL);

	/* valid literal matches */
	map_check(m, r, "/foo",     "/foo");
	map_check(m, r, "/foo/bar", "/foo/bar");
	map_check(m, r, "/foo/baz", "/foo/baz");

	/* invalid literal matches */
	map_check(m, r, "/fo",          NULL);
	map_check(m, r, "/fooo",        NULL);
	map_check(m, r, "/foo/b",       NULL);
	map_check(m, r, "/foo/barar",   NULL);
	map_check(m, r, "/foo/bar/baz", NULL);

	/* end of map building */

	/* valid t1 matches */
	map_check(m, r, "/t1", "/t1");
	fail_unless(r.empty());

	map_check(m, r, "/t1/apple", "/t1/<ID1>");
	fail_unless(r.size() == 1);
	fail_unless(r["ID1"] == "apple");

	map_check(m, r, "/t1/pear", "/t1/<ID1>");
	fail_unless(r["ID1"] == "pear");

	map_check(m, r, "/t1/pear/foo", "/t1/<ID1>/foo");
	fail_unless(r["ID1"] == "pear");

	map_check(m, r, "/t1/pear/bar", "/t1/<ID1>/bar");
	fail_unless(r["ID1"] == "pear");

	map_check(m, r, "/t1/pear/foo/dog", "/t1/<ID1>/foo/<ID2>");
	fail_unless(r["ID1"] == "pear");
	fail_unless(r["ID2"] == "dog");

	map_check(m, r, "/t1/pear/bar/dog", "/t1/<ID1>/bar/<ID2>");
	fail_unless(r["ID1"] == "pear");
	fail_unless(r["ID2"] == "dog");


	/* invalid t1 matches */
	map_check(m, r, "/t1/pear/blah", NULL);
	map_check(m, r, "/t1/pear/bar/dog/cat", NULL);

	/* valid t2 matches */
	map_check(m, r, "/t2", "/t2");
	fail_unless(r.empty());

	map_check(m, r, "/t2/apple", "/t2/<ID1>");
	fail_unless(r.size() == 1);
	fail_unless(r["ID1"] == "apple");

	map_check(m, r, "/t2/apple/dog", "/t2/<ID1>/<ID2>");
	fail_unless(r["ID1"] == "apple");
	fail_unless(r["ID2"] == "dog");

	map_check(m, r, "/t2/apple/dog/red", "/t2/<ID1>/<ID2>/<ID3>");
	fail_unless(r["ID1"] == "apple");
	fail_unless(r["ID2"] == "dog");
	fail_unless(r["ID3"] == "red");

	map_check(m, r, "/t2/fig/dog/red/f", "/t2/<ID1>/<ID2>/<ID3>/f");
	fail_unless(r["ID1"] == "fig");
	fail_unless(r["ID2"] == "dog");
	fail_unless(r["ID3"] == "red");

	map_check(m, r, "/t2/fig/dog/red/f/g", "/t2/<ID1>/<ID2>/<ID3>/f/<ID4>");
	fail_unless(r["ID1"] == "fig");
	fail_unless(r["ID2"] == "dog");
	fail_unless(r["ID3"] == "red");
	fail_unless(r["ID4"] == "g");


	/* invalid t2 matches */
	map_check(m, r, "/t2/fig/dog/red/g", NULL);
	map_check(m, r, "/t2/fig/dog/red/f/g/h", NULL);


	/* valid t3 matches */
	map_check(m, r, "/t3", "/t3");
	map_check(m, r, "/t3/fig/foo/red", "/t3/<ID1>/foo/<ID2>");


	/* invalid t3 matches */
	map_check(m, r, "/t3/fig", NULL);
	map_check(m, r, "/t3/fig/foo", NULL);
	map_check(m, r, "/t3/fig/bar/red/baz", NULL);


	/* valid greedy matches */
	map_check(m, r, "/greedy", "/greedy");

	map_check(m, r, "/greedy/pear", "/greedy/<ID1>");
	fail_unless(r.size() == 1);
	fail_unless(r["ID1"] == "pear");

	map_check(m, r, "/greedy/fig", "/greedy/<ID1>");
	fail_unless(r["ID1"] == "fig");

	map_check(m, r, "/greedy/fig/p", "/greedy/<ID1>/p/<ID2*>");
	fail_unless(r.size() == 2);
	fail_unless(r["ID1"] == "fig");
	fail_unless(r["ID2"] == "");

	map_check(m, r, "/greedy/fig/p/", "/greedy/<ID1>/p/<ID2*>");
	fail_unless(r.size() == 2);
	fail_unless(r["ID1"] == "fig");
	fail_unless(r["ID2"] == "");

	map_check(m, r, "/greedy/fig/p/aaa", "/greedy/<ID1>/p/<ID2*>");
	fail_unless(r.size() == 2);
	fail_unless(r["ID1"] == "fig");
	fail_unless(r["ID2"] == "aaa");

	map_check(m, r, "/greedy/fig/p/aaa/bbb/ccc", "/greedy/<ID1>/p/<ID2*>");
	fail_unless(r["ID1"] == "fig");
	fail_unless(r["ID2"] == "aaa/bbb/ccc");

}

TEST(uri_mapper_basic_zero)
{
	// zero prefix usage expected from nimbus

	uri_mapper m(0);
	uri_tokens r;

#define BUCKET_TOK "BUCKET"
#define OBJECT_TOK "OBJECT"

	const char *ACCOUNT_URI = "";
	const char *BUCKET_URI = "/<" BUCKET_TOK ">";
	const char *OBJECT_URI = "/<" BUCKET_TOK ">/<" OBJECT_TOK "+>";

	map_add(m, ACCOUNT_URI);
	map_add(m, BUCKET_URI);
	map_add(m, OBJECT_URI);

	/* valid account match */
	map_check(m, r, "", ACCOUNT_URI);
	fail_unless(r.size() == 0);

	/* valid bucket matches */
	map_check(m, r, "/bucket1", BUCKET_URI);
	fail_unless(r.size() == 1);
	fail_unless(r[BUCKET_TOK] == "bucket1");

	map_check(m, r, "/bucket2", BUCKET_URI);
	fail_unless(r.size() == 1);
	fail_unless(r[BUCKET_TOK] == "bucket2");

	/* valid object matches */
	map_check(m, r, "/bucket1/object1", OBJECT_URI);
	fail_unless(r.size() == 2);
	fail_unless(r[BUCKET_TOK] == "bucket1");
	fail_unless(r[OBJECT_TOK] == "object1");

	map_check(m, r, "/bucket1/object2", OBJECT_URI);
	fail_unless(r.size() == 2);
	fail_unless(r[BUCKET_TOK] == "bucket1");
	fail_unless(r[OBJECT_TOK] == "object2");

	map_check(m, r, "/bucket1/a/b", OBJECT_URI);
	fail_unless(r[BUCKET_TOK] == "bucket1");
	fail_unless(r[OBJECT_TOK] == "a/b");

	map_check(m, r, "/bucket1/a/b/c", OBJECT_URI);
	fail_unless(r[BUCKET_TOK] == "bucket1");
	fail_unless(r[OBJECT_TOK] == "a/b/c");

}
