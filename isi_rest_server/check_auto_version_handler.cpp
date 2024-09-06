#include <isi_util_cpp/check_leak_primer.hpp>
#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>

#include "auto_version_handler.h"

SUITE_DEFINE_FOR_FILE(auto_version_handler);

// Helper function for printing JSON schema
// Useful while debugging
static void
print_json_value(Json::Value& schema)
{
	Json::StyledStreamWriter writer("    ");
	writer.write(std::cout, schema);
}

// Schemas
static Json::Value
schema_number()
{
	Json::Value schema(Json::objectValue);
	schema["type"] = "number";
	return schema;
}

static Json::Value
schema_integer()
{
	Json::Value schema(Json::objectValue);
	schema["type"] = "integer";
	return schema;
}

static Json::Value
schema_null()
{
	Json::Value schema(Json::objectValue);
	schema["type"] = "null";
	return schema;
}

static Json::Value
schema_string()
{
	Json::Value schema(Json::objectValue);
	schema["type"] = "string";
	return schema;
}

static Json::Value
schema_boolean()
{
	Json::Value schema(Json::objectValue);
	schema["type"] = "boolean";
	return schema;
}

static Json::Value
schema_any()
{
	Json::Value schema(Json::objectValue);
	schema["type"] = "any";
	return schema;
}

static Json::Value
schema_array(Json::Value items_schema)
{
	Json::Value schema(Json::objectValue);
	schema["type"] = "array";
	schema["items"] = items_schema;
	return schema;
}

static Json::Value
schema_type_array_null(Json::Value other)
{
	Json::Value schema(Json::objectValue);

	Json::Value type_array(Json::arrayValue);
	type_array.append("null");
	type_array.append(other);

	schema["type"] = type_array;
	return schema;
}

static Json::Value
schema_object_simple()
{
	Json::Value schema(Json::objectValue);
	schema["type"] = "object";

	Json::Value properties(Json::objectValue);

	properties["a"] = schema_number();
	properties["b"] = schema_integer();
	properties["c"] = schema_null();
	properties["d"] = schema_string();
	properties["e"] = schema_boolean();
	properties["f"] = schema_any();

	schema["properties"] = properties;
	return schema;
}

static Json::Value
schema_object_complex()
{
	Json::Value schema(Json::objectValue);
	schema["type"] = "object";

	Json::Value properties(Json::objectValue);

	properties["a"] = schema_number();
	properties["b"] = schema_integer();
	properties["c"] = schema_null();
	properties["d"] = schema_string();
	properties["e"] = schema_boolean();
	properties["f"] = schema_any();

	properties["g"] = schema_object_simple();
	properties["h"] = schema_type_array_null(schema_object_simple());
	properties["i"] = schema_array(schema_integer());
	properties["j"] = schema_array(schema_object_simple());
	properties["k"] = schema_array(
	    schema_type_array_null(schema_object_simple()));

	schema["properties"] = properties;
	return schema;
}

static Json::Value
schema_type_array_one_obj_one_array()
{
	Json::Value schema(Json::objectValue);

	Json::Value type_array(Json::arrayValue);
	type_array.append("string");
	type_array.append("null");
	type_array.append(schema_object_simple());
	type_array.append(schema_array(schema_object_simple()));

	schema["type"] = type_array;
	return schema;
}

static Json::Value
schema_type_array_two_obj_one_array()
{
	Json::Value schema(Json::objectValue);

	Json::Value type_array(Json::arrayValue);
	type_array.append("string");
	type_array.append("null");
	type_array.append(schema_object_simple());
	type_array.append(schema_object_complex());
	type_array.append(schema_array(schema_object_simple()));

	schema["type"] = type_array;
	return schema;
}

static Json::Value
schema_type_array_one_obj_two_array()
{
	Json::Value schema(Json::objectValue);

	Json::Value type_array(Json::arrayValue);
	type_array.append("string");
	type_array.append("null");
	type_array.append(schema_object_simple());
	type_array.append(schema_array(schema_object_simple()));
	type_array.append(schema_array(schema_object_complex()));

	schema["type"] = type_array;
	return schema;
}

static Json::Value
schema_collection_response(Json::Value items_schema)
{
	Json::Value schema(Json::objectValue);
	schema["type"] = "object";

	Json::Value properties(Json::objectValue);

	properties["items"] = schema_array(items_schema);
	properties["total"] = schema_integer();
	properties["resume"] = schema_type_array_null(schema_string());

	schema["properties"] = properties;
	return schema;
}

static Json::Value
schema_error()
{
	Json::Value schema(Json::objectValue);
	schema["type"] = "object";

	Json::Value properties(Json::objectValue);

	properties["field"] = schema_string();
	properties["message"] = schema_string();
	properties["code"] = schema_string();

	schema["properties"] = properties;
	return schema;
}

static Json::Value
schema_errors_array()
{
	Json::Value schema(Json::objectValue);
	schema["type"] = "object";

	Json::Value properties(Json::objectValue);

	properties["errors"] = schema_array(schema_error());

	schema["properties"] = properties;
	return schema;
}

static Json::Value
schema_errors_array_main(Json::Value main_schema)
{
	Json::Value schema(Json::objectValue);

	Json::Value type_array(Json::arrayValue);
	type_array.append(schema_errors_array());
	type_array.append(main_schema);

	schema["type"] = type_array;
	return schema;
}

// Data
static Json::Value
data_object_simple_full()
{
	Json::Value data(Json::objectValue);
	data["a"] = 1;
	data["b"] = "sdfs";
	data["c"] = 7.980;
	data["d"] = true;
	data["e"] = Json::nullValue;
	data["f"] = Json::arrayValue;

	return data;
}

static Json::Value
data_object_simple_partial()
{
	Json::Value data(Json::objectValue);
	data["a"] = 1;
	data["b"] = "sdfs";
	data["c"] = 7.980;

	return data;
}

static Json::Value
data_object_simple_extra()
{
	Json::Value data(Json::objectValue);
	data["a"] = 1;
	data["b"] = "sdfs";
	data["c"] = 7.980;
	data["d"] = true;
	data["e"] = Json::nullValue;

	data["x"] = 1;
	data["y"] = Json::arrayValue;
	data["z"] = Json::objectValue;

	return data;
}

static Json::Value
data_error_object()
{
	Json::Value data(Json::objectValue);
	data["field"] = "foo";
	data["message"] = "foo is bad";
	data["code"] = "AEC_FOO";

	return data;
}

static Json::Value
data_errors_array()
{
	Json::Value data(Json::objectValue);
	data["errors"] = Json::arrayValue;
	data["errors"].append(data_error_object());

	return data;
}

// Tests
TEST(simple_full)
{
	Json::Value schema = schema_object_simple();
	Json::Value data = data_object_simple_full();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * No fields should be removed when the object exactly matches the
	 * schema.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.isMember("f"));
	fail_unless(data.size() == 6);
}

TEST(simple_partial)
{
	Json::Value schema = schema_object_simple();
	Json::Value data = data_object_simple_partial();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * No fields should be removed when the object has fewer fields than
	 * the schema.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.size() == 3);
}

TEST(simple_extra)
{
	Json::Value schema = schema_object_simple();
	Json::Value data = data_object_simple_extra();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * The extra fields should be removed.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.size() == 5);
}

TEST(type_array_full)
{
	Json::Value schema = schema_type_array_one_obj_one_array();
	Json::Value data = data_object_simple_full();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * No fields should be removed when the object exactly matches the
	 * schema, which was chosen from an array of schemas.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.isMember("f"));
	fail_unless(data.size() == 6);
}

TEST(type_array_partial)
{
	Json::Value schema = schema_type_array_one_obj_one_array();
	Json::Value data = data_object_simple_partial();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * No fields should be removed when the object only has fields defined
	 * in the schema, which was chosen from an array of schemas.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.size() == 3);
}

TEST(type_array_extra)
{
	Json::Value schema = schema_type_array_one_obj_one_array();
	Json::Value data = data_object_simple_extra();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * The extra fields should be removed. The schema was chosen from an
	 * array of schemas.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.size() == 5);
}

TEST(type_array_object_array)
{
	Json::Value schema = schema_type_array_one_obj_one_array();

	Json::Value data(Json::arrayValue);
	data[0] = data_object_simple_full();
	data[1] = data_object_simple_partial();
	data[2] = data_object_simple_extra();
	data[3] = "an extra item";
	data[4] = 1;
	data[5] = Json::objectValue;
	data[6] = Json::arrayValue;
	data[7] = Json::nullValue;

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * An array of schemas contains one schema that accepts an array of
	 * objects.  The input is an array of objects.  Each object should have
	 * fields removed or not based on the itmes schema for the array.
	 * Extra items that are simple types should be ignored.
	 */
	fail_unless(data.size() == 8);

	fail_unless(data[0].isMember("a"));
	fail_unless(data[0].isMember("b"));
	fail_unless(data[0].isMember("c"));
	fail_unless(data[0].isMember("d"));
	fail_unless(data[0].isMember("e"));
	fail_unless(data[0].isMember("f"));
	fail_unless(data[0].size() == 6);

	fail_unless(data[1].isMember("a"));
	fail_unless(data[1].isMember("b"));
	fail_unless(data[1].isMember("c"));
	fail_unless(data[1].size() == 3);

	fail_unless(data[2].isMember("a"));
	fail_unless(data[2].isMember("b"));
	fail_unless(data[2].isMember("c"));
	fail_unless(data[2].isMember("d"));
	fail_unless(data[2].isMember("e"));
	fail_unless(data[2].size() == 5);
}

TEST(type_array_two_obj_one_array_data_obj)
{
	Json::Value schema = schema_type_array_two_obj_one_array();
	Json::Value data = data_object_simple_extra();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * An array of schemas contains two object schemas.  The most similar
	 * schema should be selected and the fields should be removed.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.size() == 5);
}

TEST(type_array_two_obj_one_array_data_array)
{
	Json::Value schema = schema_type_array_two_obj_one_array();

	Json::Value data(Json::arrayValue);
	data[0] = data_object_simple_full();
	data[1] = data_object_simple_partial();
	data[2] = data_object_simple_extra();
	data[3] = "an extra item";
	data[4] = 1;
	data[5] = Json::objectValue;
	data[6] = Json::arrayValue;
	data[7] = Json::nullValue;

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * An array of schemas contains two object schemas and one array
	 * schema.  The input is an array of objects.  Each object should have
	 * fields removed or not based on the itmes schema for the array.
	 */
	fail_unless(data.size() == 8);

	fail_unless(data[0].isMember("a"));
	fail_unless(data[0].isMember("b"));
	fail_unless(data[0].isMember("c"));
	fail_unless(data[0].isMember("d"));
	fail_unless(data[0].isMember("e"));
	fail_unless(data[0].isMember("f"));
	fail_unless(data[0].size() == 6);

	fail_unless(data[1].isMember("a"));
	fail_unless(data[1].isMember("b"));
	fail_unless(data[1].isMember("c"));
	fail_unless(data[1].size() == 3);

	fail_unless(data[2].isMember("a"));
	fail_unless(data[2].isMember("b"));
	fail_unless(data[2].isMember("c"));
	fail_unless(data[2].isMember("d"));
	fail_unless(data[2].isMember("e"));
	fail_unless(data[2].size() == 5);
}

TEST(type_array_one_obj_two_array_data_obj)
{
	Json::Value schema = schema_type_array_one_obj_two_array();
	Json::Value data = data_object_simple_extra();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * An array of schemas contains one object schema and two array
	 * schemas.  An input object should have fields removed according to
	 * the single object schema.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.size() == 5);
}

TEST(type_array_one_obj_two_array_data_array)
{
	Json::Value schema = schema_type_array_one_obj_two_array();

	Json::Value data(Json::arrayValue);
	data[0] = data_object_simple_full();
	data[1] = data_object_simple_partial();
	data[2] = data_object_simple_extra();
	data[3] = "an extra item";
	data[4] = 1;
	data[5] = Json::objectValue;
	data[6] = Json::arrayValue;
	data[7] = Json::nullValue;

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * An array of schemas contains one object schema and two array
	 * schemas.  The input is an array of objects.  No fields should be
	 * removed.
	 */
	fail_unless(data.size() == 8);

	fail_unless(data[0].isMember("a"));
	fail_unless(data[0].isMember("b"));
	fail_unless(data[0].isMember("c"));
	fail_unless(data[0].isMember("d"));
	fail_unless(data[0].isMember("e"));
	fail_unless(data[0].isMember("f"));
	fail_unless(data[0].size() == 6);

	fail_unless(data[1].isMember("a"));
	fail_unless(data[1].isMember("b"));
	fail_unless(data[1].isMember("c"));
	fail_unless(data[1].size() == 3);

	fail_unless(data[2].isMember("a"));
	fail_unless(data[2].isMember("b"));
	fail_unless(data[2].isMember("c"));
	fail_unless(data[2].isMember("d"));
	fail_unless(data[2].isMember("e"));
	fail_unless(data[2].isMember("x"));
	fail_unless(data[2].isMember("y"));
	fail_unless(data[2].isMember("z"));
	fail_unless(data[2].size() == 8);
}

TEST(complex_simple_full)
{
	Json::Value schema = schema_object_complex();
	Json::Value data = data_object_simple_full();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * No fields should be removed when the object only has fields defined
	 * in the schema.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.isMember("f"));
	fail_unless(data.size() == 6);
}

TEST(complex_simple_extra)
{
	Json::Value schema = schema_object_complex();
	Json::Value data = data_object_simple_extra();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * The extra fields should be removed.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.size() == 5);
}

TEST(complex_field_simple_extra)
{
	Json::Value schema = schema_object_complex();

	Json::Value data(Json::objectValue);
	data["g"] = data_object_simple_extra();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * The extra fields in the nested object should be removed.
	 */
	fail_unless(data.isMember("g"));
	fail_unless(data.size() == 1);

	fail_unless(data["g"].isMember("a"));
	fail_unless(data["g"].isMember("b"));
	fail_unless(data["g"].isMember("c"));
	fail_unless(data["g"].isMember("d"));
	fail_unless(data["g"].isMember("e"));
	fail_unless(data["g"].size() == 5);
}

TEST(complex_field_type_array_simple_extra)
{
	Json::Value schema = schema_object_complex();

	Json::Value data(Json::objectValue);
	data["h"] = data_object_simple_extra();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * The extra fields in the nested object should be removed.
	 */
	fail_unless(data.isMember("h"));
	fail_unless(data.size() == 1);

	fail_unless(data["h"].isMember("a"));
	fail_unless(data["h"].isMember("b"));
	fail_unless(data["h"].isMember("c"));
	fail_unless(data["h"].isMember("d"));
	fail_unless(data["h"].isMember("e"));
	fail_unless(data["h"].size() == 5);
}

TEST(complex_field_int_array)
{
	Json::Value schema = schema_object_complex();

	Json::Value list(Json::arrayValue);
	list.append(1);
	list.append(2);

	Json::Value data(Json::objectValue);
	data["i"] = list;

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * The integer array should be unchanged.
	 */
	fail_unless(data.isMember("i"));
	fail_unless(data.size() == 1);

	fail_unless(data["i"][0] == 1);
	fail_unless(data["i"][1] == 2);
	fail_unless(data["i"].size() == 2);
}

TEST(complex_field_object_array)
{
	Json::Value schema = schema_object_complex();

	Json::Value list(Json::arrayValue);
	list[0] = data_object_simple_full();
	list[1] = data_object_simple_partial();
	list[2] = data_object_simple_extra();
	list[3] = "an extra item";
	list[4] = 1;
	list[5] = Json::objectValue;
	list[6] = Json::arrayValue;
	list[7] = Json::nullValue;

	Json::Value data(Json::objectValue);
	data["j"] = list;

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * A field of a large object is defined to be an array of objects.
	 * Each input object in this array should have fields remvoed
	 * correctly.
	 */
	fail_unless(data.isMember("j"));
	fail_unless(data.size() == 1);
	fail_unless(data["j"].size() == 8);

	fail_unless(data["j"][0].isMember("a"));
	fail_unless(data["j"][0].isMember("b"));
	fail_unless(data["j"][0].isMember("c"));
	fail_unless(data["j"][0].isMember("d"));
	fail_unless(data["j"][0].isMember("e"));
	fail_unless(data["j"][0].isMember("f"));
	fail_unless(data["j"][0].size() == 6);

	fail_unless(data["j"][1].isMember("a"));
	fail_unless(data["j"][1].isMember("b"));
	fail_unless(data["j"][1].isMember("c"));
	fail_unless(data["j"][1].size() == 3);

	fail_unless(data["j"][2].isMember("a"));
	fail_unless(data["j"][2].isMember("b"));
	fail_unless(data["j"][2].isMember("c"));
	fail_unless(data["j"][2].isMember("d"));
	fail_unless(data["j"][2].isMember("e"));
	fail_unless(data["j"][2].size() == 5);
}

TEST(complex_field_type_array_object_array)
{
	Json::Value schema = schema_object_complex();

	Json::Value list(Json::arrayValue);
	list[0] = data_object_simple_full();
	list[1] = data_object_simple_partial();
	list[2] = data_object_simple_extra();
	list[3] = "an extra item";
	list[4] = 1;
	list[5] = Json::objectValue;
	list[6] = Json::arrayValue;
	list[7] = Json::nullValue;

	Json::Value data(Json::objectValue);
	data["k"] = list;

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * A field of a large object is defined to be an array of either
	 * objects or null.  Each input object in this array should have fields
	 * remvoed correctly.
	 */
	fail_unless(data.isMember("k"));
	fail_unless(data.size() == 1);
	fail_unless(data["k"].size() == 8);

	fail_unless(data["k"][0].isMember("a"));
	fail_unless(data["k"][0].isMember("b"));
	fail_unless(data["k"][0].isMember("c"));
	fail_unless(data["k"][0].isMember("d"));
	fail_unless(data["k"][0].isMember("e"));
	fail_unless(data["k"][0].isMember("f"));
	fail_unless(data["k"][0].size() == 6);

	fail_unless(data["k"][1].isMember("a"));
	fail_unless(data["k"][1].isMember("b"));
	fail_unless(data["k"][1].isMember("c"));
	fail_unless(data["k"][1].size() == 3);

	fail_unless(data["k"][2].isMember("a"));
	fail_unless(data["k"][2].isMember("b"));
	fail_unless(data["k"][2].isMember("c"));
	fail_unless(data["k"][2].isMember("d"));
	fail_unless(data["k"][2].isMember("e"));
	fail_unless(data["k"][2].size() == 5);
}

TEST(bad_schema_simple_type)
{
	Json::Value schema;
	Json::Value data = data_object_simple_full();

	schema = schema_number();
	auto_version_handler::remove_undefined_fields(data, schema);

	schema = schema_integer();
	auto_version_handler::remove_undefined_fields(data, schema);

	schema = schema_null();
	auto_version_handler::remove_undefined_fields(data, schema);

	schema = schema_string();
	auto_version_handler::remove_undefined_fields(data, schema);

	schema = schema_boolean();
	auto_version_handler::remove_undefined_fields(data, schema);

	schema = schema_any();
	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * No fields should be removed when the schema defines a simple type.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.isMember("f"));
	fail_unless(data.size() == 6);
}

TEST(bad_schema_data_object)
{
	Json::Value schema = data_object_simple_full();
	Json::Value data = data_object_simple_full();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * No fields should be removed when the schema is not a schema, and is
	 * actually a data object.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.isMember("f"));
	fail_unless(data.size() == 6);
}

TEST(collection_simple_object)
{
	Json::Value schema = schema_errors_array_main(
	    schema_collection_response(schema_object_simple()));

	Json::Value list(Json::arrayValue);
	list[0] = data_object_simple_full();
	list[1] = data_object_simple_partial();
	list[2] = data_object_simple_extra();

	Json::Value data(Json::objectValue);
	data["items"] = list;
	data["total"] = 3;
	data["resume"] = Json::nullValue;

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * A field of a large object is defined to be an array of either
	 * objects or null.  Each input object in this array should have fields
	 * remvoed correctly.
	 */
	fail_unless(data.isMember("items"));
	fail_unless(data.isMember("total"));
	fail_unless(data.isMember("resume"));
	fail_unless(data.size() == 3);
	fail_unless(data["items"].size() == 3);

	fail_unless(data["items"][0].isMember("a"));
	fail_unless(data["items"][0].isMember("b"));
	fail_unless(data["items"][0].isMember("c"));
	fail_unless(data["items"][0].isMember("d"));
	fail_unless(data["items"][0].isMember("e"));
	fail_unless(data["items"][0].isMember("f"));
	fail_unless(data["items"][0].size() == 6);

	fail_unless(data["items"][1].isMember("a"));
	fail_unless(data["items"][1].isMember("b"));
	fail_unless(data["items"][1].isMember("c"));
	fail_unless(data["items"][1].size() == 3);

	fail_unless(data["items"][2].isMember("a"));
	fail_unless(data["items"][2].isMember("b"));
	fail_unless(data["items"][2].isMember("c"));
	fail_unless(data["items"][2].isMember("d"));
	fail_unless(data["items"][2].isMember("e"));
	fail_unless(data["items"][2].size() == 5);
}

TEST(error_schema_full)
{
	Json::Value schema = schema_errors_array_main(schema_object_simple());
	Json::Value data = data_object_simple_full();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * No fields should be removed when the object exactly matches the
	 * schema.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.isMember("f"));
	fail_unless(data.size() == 6);
}

TEST(error_schema_partial)
{
	Json::Value schema = schema_errors_array_main(schema_object_simple());
	Json::Value data = data_object_simple_partial();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * No fields should be removed when the object has fewer fields than
	 * the schema.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.size() == 3);
}

TEST(error_schema_extra)
{
	Json::Value schema = schema_errors_array_main(schema_object_simple());
	Json::Value data = data_object_simple_extra();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * The extra fields should be removed.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.size() == 5);
}

/*
 * This is a negative test case. Data would never have an error because
 * the auto version handler does not get called in the error path. So we now
 * check that the data should be empty.
 */
TEST(error_schema_errors)
{
	Json::Value schema = schema_errors_array_main(schema_object_simple());
	Json::Value data = data_errors_array();

	auto_version_handler::remove_undefined_fields(data, schema);

	fail_unless(data.size() == 0);
}

TEST(error_schema_type_array_data_obj_full)
{
	Json::Value schema = schema_errors_array_main(
		schema_type_array_one_obj_one_array());
	Json::Value data = data_object_simple_full();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * No fields should be removed when the object exactly matches the
	 * schema.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.isMember("f"));
	fail_unless(data.size() == 6);
}

TEST(error_schema_type_array_data_obj_partial)
{
	Json::Value schema = schema_errors_array_main(
		schema_type_array_one_obj_one_array());
	Json::Value data = data_object_simple_partial();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * No fields should be removed when the object has fewer fields than
	 * the schema.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.size() == 3);
}

TEST(error_schema_type_array_data_obj_extra)
{
	Json::Value schema = schema_errors_array_main(
		schema_type_array_one_obj_one_array());
	Json::Value data = data_object_simple_extra();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * The extra fields should be removed.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.size() == 5);
}

TEST(additional_properties)
{
	Json::Value schema = schema_object_simple();
	schema["additionalProperties"] = true;
	Json::Value data = data_object_simple_extra();

	auto_version_handler::remove_undefined_fields(data, schema);

	/*
	 * Additional properties are explicitly allowed so don't remove fields.
	 */
	fail_unless(data.isMember("a"));
	fail_unless(data.isMember("b"));
	fail_unless(data.isMember("c"));
	fail_unless(data.isMember("d"));
	fail_unless(data.isMember("e"));
	fail_unless(data.isMember("x"));
	fail_unless(data.isMember("y"));
	fail_unless(data.isMember("z"));
	fail_unless(data.size() == 8);
}
