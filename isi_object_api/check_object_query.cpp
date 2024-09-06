#include "object_query.h"

#include <check.h>

#include <isi_rest_server/api_error.h>
#include <isi_util_cpp/check_leak_primer.hpp>

#include "handler_common.h"
#include "ob_attr_mgr.h"

TEST_FIXTURE(suite_setup);

SUITE_DEFINE_FOR_FILE(check_object_query,
    .mem_check = CK_MEM_DEFAULT,
    .dmalloc_opts =  CHECK_DMALLOC_DEFAULT,
    .suite_setup = suite_setup);

/*
 * XXX:A whole bunch of these things 'leak' statics.  I couldn't figure out
 * what all those were, so this is the workaround
 */

TEST_FIXTURE(suite_setup)
{
	Json::Reader reader;
	Json::Value value;
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	string scope("{\"operatorx\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");

	reader.parse(scope, value);
	try {
		compiler.compile(value, rule);
	}
	catch (api_exception &ex) {
	}
}

/**
 * Test the following input
 * { "operator" : "=",
 *   "attr" : "name",
 *   "value" : "my_object"
 * }
 * It should succeed
 */
TEST(test_simple_predicate_op_eq, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}

/**
 * Test the following input
 * { "operator" : "!=",
 *   "attr" : "name",
 *   "value" : "my_object"
 * }
 * It should succeed
 */
TEST(test_simple_predicate_op_not_eq, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"!=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}

/**
 * Test the following input
 * { "operator" : ">",
 *   "attr" : "name",
 *   "value" : "my_object"
 * }
 * It should succeed
 */
TEST(test_simple_predicate_op_greater, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \">\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}

/**
 * Test the following input
 * { "operator" : ">=",
 *   "attr" : "name",
 *   "value" : "my_object"
 * }
 * It should succeed
 */
TEST(test_simple_predicate_op_greater_eq, .mem_check = CK_MEM_DEFAULT)
{
	Json::Reader reader;
	string scope("{\"operator\" : \">=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}


/**
 * Test the following input
 * { "operator" : "<",
 *   "attr" : "name",
 *   "value" : "my_object"
 * }
 * It should succeed
 */
TEST(test_simple_predicate_op_less, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"<\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}

/**
 * Test the following input
 * { "operator" : "<=",
 *   "attr" : "name",
 *   "value" : "my_object"
 * }
 * It should succeed
 */
TEST(test_simple_predicate_op_less_eq, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"<=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}

/**
 * Test the following input
 * { "operator" : "like",
 *   "attr" : "name",
 *   "value" : "my_object%"
 * }
 * It should succeed
 */
TEST(test_simple_predicate_op_like, .mem_check = CK_MEM_DEFAULT,
    .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"like\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}

/**
 * Test the following input
 * { "operator" : "in",
 *   "attr" : "name",
 *   "value" : ["my_object1", "my_object2"]
 * }
 * It should succeed
 */
TEST(test_simple_predicate_op_in, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"in\", \"attr\" : \"name\", "
	    "\"value\" : [\"my_object1\", \"my_object2\"]}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}

/**
 * Test the following input
 * { "operatorx" : "=",
 *   "attr" : "name",
 *   "value" : "my_object"
 * }
 * It should fail
 */
TEST(test_bad_operator_key, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operatorx\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	bool failed = false;
	try {
		compiler.compile(scope_query, rule);
	}
	catch (api_exception &ex) {
		failed = true;
	}
	
	fail_unless(failed, "Exception is expected");
}

/**
 * Test the following input
 * { "operatorx" : "==",
 *   "attr" : "name",
 *   "value" : "my_object"
 * }
 * It should fail
 */
TEST(test_bad_operator_value, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"==\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	bool failed = false;
	try {
		compiler.compile(scope_query, rule);
	}
	catch (api_exception &ex) {
		failed = true;
	}
	
	fail_unless(failed, "Exception is expected");
}


/**
 * Test the following input
 * { "operatorx" : "",
 *   "attr" : "name",
 *   "value" : "my_object"
 * }
 * It should fail
 */
TEST(test_empty_operator_value, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	bool failed = false;
	try {
		compiler.compile(scope_query, rule);
	}
	catch (api_exception &ex) {
		failed = true;
	}
	
	fail_unless(failed, "Exception is expected");
}

/**
 * Test the following input
 * { "operator" : "=",
 *   "attrx" : "name",
 *   "value" : "my_object"
 * }
 * It should fail.
 */
TEST(test_miss_attr_field, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"==\", \"attrx\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	bool failed = false;
	try {
		compiler.compile(scope_query, rule);
	}
	catch (api_exception &ex) {
		failed = true;
	}
	
	fail_unless(failed, "Exception is expected");
}


/**
 * Test the following input
 * { "operator" : "=",
 *   "attr" : "",
 *   "value" : "my_object"
 * }
 * It should fail.
 */
TEST(test_miss_attr_value, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"==\", \"attrx\" : \"\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	bool failed = false;
	try {
		compiler.compile(scope_query, rule);
	}
	catch (api_exception &ex) {
		failed = true;
	}
	
	fail_unless(failed, "Exception is expected");
}

/**
 * Test the following input
 * { "operator" : "=",
 *   "attr" : "",
 *   "valuex" : "my_object"
 * }
 * It should fail.
 */
TEST(test_miss_value_key, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"==\", \"attrx\" : \"\", "
	    "\"valuex\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	bool failed = false;
	try {
		compiler.compile(scope_query, rule);
	}
	catch (api_exception &ex) {
		failed = true;
	}
	
	fail_unless(failed, "Exception is expected");
}

/**
 * Test the following input
 * { "operator" : "like",
 *   "attr" : "name",
 *   "value" : ""
 * }
 * It should fail -- like should have a non empty value
 */
TEST(test_empty_like_value, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"like\", \"attr\" : \"name\", "
	    "\"value\" : \"\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	bool failed = false;
	try {
		compiler.compile(scope_query, rule);
	}
	catch (api_exception &ex) {
		failed = true;
	}
	
	fail_unless(failed, "Exception is expected: "
	    "like cannot have empty value");
}

/**
 * Test the following input with a logic and
 * {
 * 	"logic" : "and",
 * 	"conditions" : [
 * 	{ 
 * 		"operator" : "=",
 *   		"attr" : "name",
 *   		"value" : "my_object"
 * 	}
 * 	]
 * }
 * It should succeed
 */
TEST(test_simple_and_logic, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"logic\" : \"and\", "
	    "\"conditions\": [{\"operator\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}]}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}

/**
 * Test the following input with a logic or
 * {
 * 	"logic" : "or",
 * 	"conditions" : [
 * 	{ 
 * 		"operator" : "=",
 *   		"attr" : "name",
 *   		"value" : "my_object"
 * 	}
 * 	]
 * }
 * It should succeed
 */
TEST(test_simple_or_logic, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"logic\" : \"or\", "
	    "\"conditions\": [{\"operator\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}]}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}


/**
 * Test the following input with a logic and
 * {
 * 	"logic" : "and",
 * 	"conditions" : [
 * 	{ 
 * 		"operator" : "=",
 *   		"attr" : "name",
 *   		"value" : "my_object"
 * 	},
 * 	{ 
 * 		"operator" : "like",
 *   		"attr" : "user.color",
 *   		"value" : "green%"
 * 	}
 * 	]
 * }
 * It should succeed
 */
TEST(test_simple_two_and_logic, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"logic\" : \"and\", "
	    "\"conditions\": ["
	    "{\"operator\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}, "
	    "{\"operator\" : \"like\", \"attr\" : \"user.color\", "
	    "\"value\" : \"green%\"}"
	    "]}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}

/**
 * Test the following input with a logic or
 * {
 * 	"logic" : "or",
 * 	"conditions" : [
 * 	{ 
 * 		"operator" : "=",
 *   		"attr" : "name",
 *   		"value" : "my_object"
 * 	},
 * 	{ 
 * 		"operator" : "like",
 *   		"attr" : "user.color",
 *   		"value" : "green%"
 * 	}
 * 	]
 * }
 * It should succeed
 */
TEST(test_simple_two_or_logic, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"logic\" : \"or\", "
	    "\"conditions\": ["
	    "{\"operator\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}, "
	    "{\"operator\" : \"like\", \"attr\" : \"user.color\", "
	    "\"value\" : \"green%\"}"
	    "]}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}


/**
 * Test the following input with a logic or
 * {
 * 	"logic" : "not",
 * 	"conditions" : [
 * 	{ 
 * 		"operator" : "=",
 *   		"attr" : "name",
 *   		"value" : "my_object"
 * 	}
 * 	]
 * }
 * It should succeed
 */
TEST(test_simple_not_logic, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"logic\" : \"not\", "
	    "\"conditions\": [{\"operator\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}]}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
}

/**
 * Test the following input with a logic or
 * {
 * 	"logic" : "and",
 * 	"conditionsx" : [
 * 	{ 
 * 		"operator" : "=",
 *   		"attr" : "name",
 *   		"value" : "my_object"
 * 	}
 * 	]
 * }
 * It should succeed
 */
TEST(test_no_conditions_logic, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"logic\" : \"and\", "
	    "\"conditionsx\": [{\"operator\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}]}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: there must be conditions field");
	}
	catch (api_exception &ex) {
	}
}

/**
 * Test the following input with a logic or
 * {
 * 	"logic" : "and",
 * 	"conditions" : 
 * 	{ 
 * 		"operator" : "=",
 *   		"attr" : "name",
 *   		"value" : "my_object"
 * 	}
 * }
 * It should fail
 */
TEST(test_conditions_must_be_array_logic, .mem_check = CK_MEM_DEFAULT, 
    .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"logic\" : \"and\", "
	    "\"conditions\": {\"operator\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: the conditions value must "
		    "be an array");
	}
	catch (api_exception &ex) {
	}
}

/**
 * Test the following input with a logic or
 * {
 * 	"logic" : "and",
 * 	"conditions" : [] 
 * }
 * It should fail
 */
TEST(test_conditions_array_may_not_be_empty_logic, 
    .mem_check = CK_MEM_DEFAULT, 
    .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"logic\" : \"and\", "
	    "\"conditions\": []}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: the conditions value array "
		    "must have at least one element.");
	}
	catch (api_exception &ex) {
	}
}

/**
 * Test the following input with a logic not
 * {
 * 	"logic" : "not",
 * 	"conditions" : [
 * 	{ 
 * 		"operator" : "=",
 *   		"attr" : "name",
 *   		"value" : "my_object"
 * 	},
 * 	{ 
 * 		"operator" : "like",
 *   		"attr" : "user.color",
 *   		"value" : "green%"
 * 	}
 * 	]
 * }
 * It should fail
 */
TEST(test_not_is_unary_logic, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"logic\" : \"not\", "
	    "\"conditions\": ["
	    "{\"operator\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}, "
	    "{\"operator\" : \"like\", \"attr\" : \"user.color\", "
	    "\"value\" : \"green%\"}"
	    "]}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: the conditions value array "
		    "must have at one element for not logic.");
	}
	catch (api_exception &ex) {
	}
}


/**
 * Test the following input with nested logic
 * {
 * 
 * {
 *             “logic” : “or”,
 *             “conditions” : 
 *		[
 *		{
 *                            “operator” : “=”,
 *                            “attr” : “user.manufacture”,
 *                            “value” : “ACME”
 *		},
 *              {
 *                            “logic” : “and”,
 *                            “conditions” : 
 *                            [
 *                            {
 *                            “operator” : “=”,
 *                            “attr” : “user.model”,
 *                            “value” : “T750”
 *                            },
 *                            {
 *                            “operator” : “=”,
 *                            “attr” : “user.color”,
 *                            “value” : “black”
 *                            }
 *                            ]
 *             }
 *	       ]
 *  }
 * It should succeed
 */
TEST(test_nested_logic, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"logic\" : \"or\", "
	    "\"conditions\": ["
	    "{\"operator\" : \"=\", \"attr\" : \"user.manufacture\", "
	    "\"value\" : \"ACME\"}, "
	    "{ \"logic\" : \"and\","
	    "\"conditions\": ["
	    "{\"operator\" : \"=\", \"attr\" : \"user.model\", "
	    "\"value\" : \"T750\"}, "
	    "{\"operator\" : \"=\", \"attr\" : \"user.color\", "
	    "\"value\" : \"black\"}]}"	    
	    "]}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);

	dt_object obj(idt_class::instance());
	obj.set_attr("user.manufacture", "ACME");
	fail_unless(rule.evaluate(obj), 
	    "The object should satisfy the condition");

	obj.clear();
	obj.set_attr("user.model", "T750");
	fail_if(rule.evaluate(obj), 
	    "The object should not satisfy the condition");
	
	obj.clear();	
	obj.set_attr("user.color", "black");
	fail_if(rule.evaluate(obj), 
	    "The object should not satisfy the condition");
	
	// set both conditions:
	obj.clear();
	obj.set_attr("user.color", "black");
	obj.set_attr("user.model", "T750");
	fail_unless(rule.evaluate(obj), 
	    "The object should satisfy the condition");
	    
}

/*
 * Test the rule engine:
 */

/**
 * Test the following input
 * { "operator" : "=",
 *   "attr" : "name",
 *   "value" : "my_object"
 * }
 * It should succeed
 */
TEST(test_simple_query, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
	
	dt_object obj(idt_class::instance());
	obj.set_attr("name", "my_object");
	fail_unless(rule.evaluate(obj), 
	    "The object should satisfy the condition");
}

/**
 * Test the following input
 * { "operator" : "=",
 *   "attr" : "name",
 *   "value" : "my_object"
 * }
 * It should fail as the object is named differently
 */
TEST(test_simple_query_not_found, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"=\", \"attr\" : \"name\", "
	    "\"value\" : \"my_object\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
	
	dt_object obj(idt_class::instance());
	obj.set_attr("name", "my_object2");
	fail_if(rule.evaluate(obj), 
	    "The object should not satisfy the condition");
}


/**
 * Test the following input
 * { "operator" : "like",
 *   "attr" : "name",
 *   "value" : "my_object.*"
 * }
 * It should succeed
 */
TEST(test_simple_query_like, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"like\", \"attr\" : \"name\", "
	    "\"value\" : \"^my_object.*\"}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
	
	dt_object obj(idt_class::instance());
	obj.set_attr("name", "my_object_is_great");
	fail_unless(rule.evaluate(obj), 
	    "The object should satisfy the condition");
	
	obj.clear();
	obj.set_attr("name", "xmy_object_is_great");
	fail_if(rule.evaluate(obj), 
	    "Like is broken: The object should not satisfy the condition");	
}

/**
 * Test the following input
 * { "operator" : "in",
 *   "attr" : "name",
 *   "value" : ["my_object1", "my_object2"]
 * }
 * Then try to match objects with the attribute
 * It should succeed
 */
TEST(test_simple_query_op_in, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"in\", \"attr\" : \"name\", "
	    "\"value\" : [\"my_object1\", \"my_object2\"]}");
	
	Json::Value scope_query;
	reader.parse(scope, scope_query);
	
	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	compiler.compile(scope_query, rule);
	
	dt_object obj(idt_class::instance());
	obj.set_attr("name", "my_object1");
	fail_unless(rule.evaluate(obj), 
	    "The object should satisfy the condition");

	obj.set_attr("name", "my_object2");
	fail_unless(rule.evaluate(obj), 
	    "The object should satisfy the condition");
	
	obj.set_attr("name", "my_object3");
	fail_if(rule.evaluate(obj), 
	    "The object should not satisfy the condition");
}

/**
 * Check different operators on owner
 *
 */
TEST(test_isi_owner, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;
	string scope("{\"operator\" : \"like\", \"attr\" : \"owner\", "
	    "\"value\" : \"\"}");

	Json::Value scope_query;
	reader.parse(scope, scope_query);

	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "owner does not support like");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \"<\", \"attr\" : \"owner\", "
	    "\"value\" : \"\"}";

	reader.parse(scope, scope_query);
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "owner does not support <");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \"<=\", \"attr\" : \"owner\", "
	    "\"value\" : \"\"}";

	reader.parse(scope, scope_query);
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "owner does not support <=");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \">\", \"attr\" : \"owner\", "
	    "\"value\" : \"\"}";

	reader.parse(scope, scope_query);
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "owner does not support >");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \">=\", \"attr\" : \"owner\", "
	    "\"value\" : \"\"}";

	reader.parse(scope, scope_query);
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "owner does not support >=");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \"=\", \"attr\" : \"owner\", "
	    "\"value\" : \"\"}";

	reader.parse(scope, scope_query);
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "owner must be valid");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \"=\", \"attr\" : \"owner\", "
	    "\"value\" : 0}";

	reader.parse(scope, scope_query);
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "owner must be valid");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \"=\", \"attr\" : \"owner\", "
	    "\"value\" : \"root\"}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	jdt_object obj(idt_class::instance());
	obj.set_attr(ISI_OWNER, (uid_t)0);
	fail_unless(rule.evaluate(obj),
	    "The object should satisfy the condition");

}

/**
 * Check different operators on last_modified
 *
 */
TEST(test_isi_mtime, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;

	string scope("{\"operator\" : \"like\", \"attr\" : \"last_modified\", "
	    "\"value\" : \"Thu, 15 Dec 2011 06:41:0\"}");

	Json::Value scope_query;
	reader.parse(scope, scope_query);

	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "owner does not support like");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \"=\", \"attr\" : \"last_modified\", "
	    "\"value\" : \"Thux, 15 Dec 2011 06:41:0\"}";

	reader.parse(scope, scope_query);
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "The date is not valid");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \"<\", \"attr\" : \"last_modified\", "
	    "\"value\" : \"Thu, 15 Dec 2011 06:41:0\"}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	rule.cleanup();
	scope =	"{\"operator\" : \"<=\", \"attr\" : \"last_modified\", "
	    "\"value\" : \"Thu, 15 Dec 2011 06:41:0\"}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	rule.cleanup();
	scope =	"{\"operator\" : \">\", \"attr\" : \"last_modified\", "
	    "\"value\" : \"Thu, 15 Dec 2011 06:41:0\"}";

	reader.parse(scope, scope_query);
		compiler.compile(scope_query, rule);

	rule.cleanup();
	scope =	"{\"operator\" : \">=\", \"attr\" : \"last_modified\", "
	    "\"value\" : \"Thu, 15 Dec 2011 06:41:0\"}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	rule.cleanup();
	scope =	"{\"operator\" : \"in\", \"attr\" : \"last_modified\", "
	    "\"value\" : [\"Thu, 15 Dec 2011 06:41:0\", "
	    "\"Fri, 16 Dec 2011 00:00:0\"]}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	rule.cleanup();
	scope =	"{\"operator\" : \"in\", \"attr\" : \"last_modified\", "
	    "\"value\" : [12, 23431212]}";

	reader.parse(scope, scope_query);
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "The date is not valid");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \">\", \"attr\" : \"last_modified\", "
	    "\"value\" : \"Thu, 15 Dec 2011 06:41:0\"}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	jdt_object obj(idt_class::instance());
	obj.set_attr(ISI_MTIME, (uid_t)0);
	fail_unless(rule.evaluate(obj),
	    "The object should satisfy the condition");

}

/**
 * Check different operators on size
 *
 */
TEST(test_isi_size, .mem_check = CK_MEM_DEFAULT, .mem_hint = 0)
{
	Json::Reader reader;

	string scope("{\"operator\" : \"like\", \"attr\" : \"size\", "
	    "\"value\" : \"123\"}");

	Json::Value scope_query;
	reader.parse(scope, scope_query);

	scope_compiler compiler(ob_attr_mgr::get_attr_mgr());
	scope_rule rule;
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "size does not support like");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \"=\", \"attr\" : \"size\", "
	    "\"value\" : \"123\"}";

	reader.parse(scope, scope_query);
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "The size mut be an integer");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \"<\", \"attr\" : \"size\", "
	    "\"value\" : 1234}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	rule.cleanup();
	scope =	"{\"operator\" : \"<=\", \"attr\" : \"size\", "
	    "\"value\" : 1234}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	rule.cleanup();
	scope =	"{\"operator\" : \">\", \"attr\" : \"size\", "
	    "\"value\" : 1234}";

	reader.parse(scope, scope_query);
		compiler.compile(scope_query, rule);

	rule.cleanup();
	scope =	"{\"operator\" : \">=\", \"attr\" : \"size\", "
	    "\"value\" : 1234}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	rule.cleanup();
	scope =	"{\"operator\" : \"in\", \"attr\" : \"size\", "
	    "\"value\" : [1234, 5678]}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	rule.cleanup();
	scope =	"{\"operator\" : \"in\", \"attr\" : \"size\", "
	    "\"value\" : [\"12\", \"23431212\"]}";

	reader.parse(scope, scope_query);
	try {
		compiler.compile(scope_query, rule);
		fail("Exception is expected: "
		    "The size element must be integer");
	}
	catch (api_exception &ex) {
	}

	rule.cleanup();
	scope =	"{\"operator\" : \">\", \"attr\" : \"size\", "
	    "\"value\" : 123}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	jdt_object obj(idt_class::instance());
	obj.set_attr(ISI_SIZE, (size_t)456);
	fail_unless(rule.evaluate(obj),
	    "The object should satisfy the condition");

	rule.cleanup();
	scope =	"{\"operator\" : \"=\", \"attr\" : \"size\", "
	    "\"value\" : 321}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	obj.set_attr(ISI_SIZE, 321);
	fail_unless(rule.evaluate(obj),
	    "The object should satisfy the condition");

	rule.cleanup();
	scope =	"{\"operator\" : \"=\", \"attr\" : \"size\", "
	    "\"value\" : 4294967297}";

	reader.parse(scope, scope_query);
	compiler.compile(scope_query, rule);

	obj.set_attr(ISI_SIZE, (Json::UInt64)4294967297ull);
	fail_unless(rule.evaluate(obj),
	    "The object should satisfy the condition");

}


