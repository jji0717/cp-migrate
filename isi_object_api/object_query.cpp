#include "object_query.h"

#include <regex.h>

#include <isi_rest_server/api_error.h>
#include <isi_ilog/ilog.h>
#include <isi_util_cpp/http_util.h>
#include <isi_persona/persona_util.h>


#include "handler_common.h"

namespace {

const char *QUERY_SCOPE		= "scope";
const char *QUERY_RESULT	= "result";
const char *QUERY_LOGICAL 	= "logic";
const char *QUERY_OPERATOR 	= "operator";
const char *QUERY_ATTR		= "attr";
const char *QUERY_VALUE		= "value";
const char *QUERY_CONDITIONS	= "conditions";

// query operators strings:
const char *QOS_EQ		= "=";
const char *QOS_NOT_EQ		= "!=";
const char *QOS_LESS		= "<";
const char *QOS_LESS_EQ		= "<=";
const char *QOS_GREATER		= ">";
const char *QOS_GREATER_EQ	= ">=";
const char *QOS_LIKE		= "like";
const char *QOS_IN		= "in";

// query logical operators:
const char *QLS_AND		= "and";
const char *QLS_OR		= "or";
const char *QLS_NOT		= "not";

/**
 * Check if the str is like the pattern given.
 * This implements the "like" operator.
 * @param[in] str - the string being checked
 * @param[in] pattern - the pattern string in regular expression
 * @param[in] icase - do case insensitive compare
 * @return true if matched, false otherwise
 */
bool
is_like(const std::string &str, const std::string pattern, bool icase)
{
	regex_t re;
	int status = 0;
	int cflags = REG_EXTENDED | REG_NOSUB;
	if (icase)
		cflags |= REG_ICASE;
	if ((status = regcomp(&re, pattern.c_str(), cflags)) != 0) {
		ilog(IL_DEBUG, "regcomp failed: %d str:%s, pattern:%s", 
		    status, str.c_str(), pattern.c_str());
		return false;
	}
	
	status = regexec(&re, str.c_str(), 0, NULL, 0);
	
	if (status) {
		ilog(IL_DEBUG, "regexec failed: %d str:%s, pattern:%s", 
		    status, str.c_str(), pattern.c_str());
	}
	
	regfree(&re);
	
	return status == 0; // status = 0 means it is matched.
}

/**
 * Check if the string is in the value in the in_list
 */
bool
is_in(const Json::Value &val, const Json::Value in_list)
{
	bool matched = false;
	Json::Value::const_iterator it;
	for (it = in_list.begin(); it != in_list.end(); ++it) {
		const Json::Value & item = *it;
		
		if (val == item) {
			matched = true;
			break;
		}
	}
	
	return matched;
}

/**
 * Get the UID for the specified user
 */
bool
get_uid_for_user(const char *user, uid_t &uid)
{
	ASSERT(user);

	persona *per = NULL;
	per = name_to_persona(user, true, PERSONA_USE_OWNER);

	if (!per) {
		ilog(IL_DEBUG, "Failed to get persona for user %s error=%d",
		    user, errno);
		return false;
	}

	int err = persona_get_uid(per, &uid);

	persona_free(per);

	return err == 0;
}

/**
 * Get the GID for the specified user
 */
bool
get_gid_for_group(const char *group, uid_t &gid)
{
	ASSERT(group);

	persona *per = NULL;
	per = name_to_persona(group, true, PERSONA_USE_GROUP);

	if (!per) {
		ilog(IL_DEBUG, "Failed to get persona for group %s error=%d",
		    group, errno);
		return false;
	}

	int err = persona_get_gid(per, &gid);

	persona_free(per);

	return err == 0;
}

}

void
dt_object::set_attr(const string &key, const Json::Value &value)
{
	attr_map_[key] = value;
}

bool
dt_object::get_attr(const string &key, Json::Value &value) const
{
	map<const string, Json::Value>::const_iterator it;
	it = attr_map_.find(key);
	if (it == attr_map_.end())
		return false;
		
	value = it->second;
	return true;
}

const Json::Value *
dt_object::get_attr(const string &key) const
{
	map<const string, Json::Value>::const_iterator it;
	it = attr_map_.find(key);
	if (it == attr_map_.end())
		return NULL;

	return &(it->second);
}

void
jdt_object::set_attr(const string &key, const Json::Value &value)
{
	attr_map_[key] = value;
}

bool
jdt_object::get_attr(const string &key, Json::Value &value) const
{
	value = attr_map_[key];
	return !value.isNull();
}

const Json::Value *
jdt_object::get_attr(const string &key) const
{
	if(attr_map_.isMember(key))
		return &attr_map_[key];
	return NULL;
}

/**
 * add a child rule node to the logical node.
 */
void
logical_node::add_child(rule_node *child)
{
	children_.push_back(child);
}

logical_node::~logical_node()
{
	std::list<rule_node *>::iterator it;
	for (it = children_.begin(); it != children_.end(); ++it) {
		rule_node *node = *it;
		ASSERT(node);
		delete node;
	}
	children_.clear();
}

bool
predicate_node::evaluate(const idt_object &obj) const
{
	// if the attribute being evaluated does not apply to the class
	// return true -- ignore the condition.
	if (!obj.is_applicable(attr_)) {
		return true;
	}
	
	bool matched = false;
	Json::Value val;
	bool found = obj.get_attr(attr_, val);
	
	switch (comp_op_) {
	case CO_EQUAL: {
		if ( found && val == value_)
			matched = true;
		break;
	}
	case CO_NOTEQUAL: {
		if (found && val != value_)
			matched = true;
		break;
	}
	case CO_GREATER: {
		if (found && val > value_)
			matched = true;
		break;
	}
	case CO_GREATER_EQ: {
		if (found && val >= value_)
			matched = true;
		break;		
	}
	case CO_LESS: {
		if (found && val < value_)
			matched = true;
		break;
	}
	case CO_LESS_EQ: {
		if (found && val <= value_)
			matched = true;
		break;		
	}
	case CO_LIKE: {
		const string & op = value_.asString();
		matched = is_like(val.asString(), op, false);
		break;		
	}
	case CO_IN: {
		matched = is_in(val, value_);
		break;		
	}
	default:
		throw api_exception(AEC_EXCEPTION,
		    "Wrong comparision operator.");
		break;			
	}
	
	return matched;
}

bool
logical_node::evaluate(const idt_object &obj) const
{
	bool matched = false;
	std::list<rule_node *>::const_iterator it;
	for (it = children_.begin(); it != children_.end(); ++it) {
		const rule_node *node = *it;
		ASSERT(node);
		matched = node->evaluate(obj);
		
		if (matched && logic_op_ == LO_OR) 
			break;
		if (!matched && logic_op_ == LO_AND)
			break;
		if (logic_op_ == LO_NOT) {
			matched = !matched;
			break;
		}
	}
	
	return matched;
}

/*
 * Add a logical node to the rule
 */
logical_node *
scope_rule::add_logical(
    LOGICAL_OPERATOR op, 
    logical_node *parent)
{
	logical_node *new_node = new logical_node(op, parent);
	
	if (root_ == NULL) {
		ASSERT(parent == NULL);
		root_ = new_node;
		return new_node;
	}
	
	// must have a parent node
	ASSERT(parent);
	parent->add_child(new_node);
	return new_node;
}

/*
 * Add a predicate node to the rule
 */
predicate_node *
scope_rule::add_predicate(
    const string &attr,
    COMPARE_OPERATOR co,
    const Json::Value &value,
    logical_node *parent)
{
	predicate_node *new_node = 
	    new predicate_node(attr, co, value, parent);
	
	if (root_ == NULL) {
		ASSERT(parent == NULL);
		root_ = new_node;
		return new_node;
	}
	
	// must have a parent node
	ASSERT(parent);
	parent->add_child(new_node);
	return new_node;
}

/*
 * Public function to add an "And predicate" into the Root
 * If the root is already an And logic, just add child predicate. Otherwise,
 * create a new AND logic root and add the the new predicate and the
 * old root as the children of the new root.
 *
 * This routine is useful only in the case that the system wants to piggy-
 * back ride the JSON query.
 */
void
scope_rule::add_predicate(const string &attr,
    COMPARE_OPERATOR co,
    const Json::Value &value)
{
	if (root_ == NULL) {
		add_predicate(attr, co, value, NULL);
		return;
	}

	bool add_new_root = false;
	if (root_->node_type() == RNT_LOGIC) {
		logical_node * node = dynamic_cast<logical_node *>(root_);
		if (node->logic() != LO_AND)
			add_new_root = true;
	}
	else
		add_new_root = true;

	if (add_new_root) {
		rule_node *old_root = root_;
		root_ = NULL;
		scope_rule::add_logical(LO_AND, NULL);
		ASSERT(root_ != NULL);
		logical_node * node = dynamic_cast<logical_node *>(root_);
		// cppcheck-suppress nullPointer 
		node->add_child(old_root);
		add_predicate(attr, co, value, node);
	}
	else {
		logical_node * node = dynamic_cast<logical_node *>(root_);
		add_predicate(attr, co, value, node);
	}
}

void
scope_rule::cleanup()
{
	// clean up the resources
	if (root_) {
		delete root_;
		root_ = NULL;
	}

}

scope_rule::~scope_rule()
{
	cleanup();
}

bool 
scope_rule::evaluate(const idt_object &object) const
{	
	if (root_ == NULL)
		return true;
	
	rule_node * node = root_;
	return node->evaluate(object);	
}

/*
 * Compile the JSON scope specification to binary rule structure
 * @param[in] scope - JSON scope query specification
 * The scope_query is defined as:
 *
 * scope_query = predicate |
 * {
 * 	“logic” : “<logic_operator>”,
 * 	“conditions” :  [<condition>]
 * }
 *
 * logic_operator = and|or|not
 * The only logical operator supported is “and”, “or” and “not”. 
 * Where “not” is a unary operator and one only one condition is 
 * valid. This operator essentially negates the condition evaluated
 * in conditions.
 * For “and” and “or” operator, two or more conditions should 
 * be specified in the conditions value.
 * The “conditions” value consists of an array of conditions. 
 * Each condition is defined as:	
 * condition = scope_query|predicate.	
 * And “predicate” is defined as,
 *
 * predicate =
 * {
 * 	“operator” : “<comparison_operator>”,
 * 	“attr” : “attr_name”,
 * 	“value” : “attr_value” | string_array
 * }
 *
 * <comparison_operator> = =|!=|<|<=|>|>=|like|in
 *
 * The “like” operator will match the specified attribute with a pattern.
 *
 * If the operator is “in”, the value must be an array of strings. 
 * At least one element must be in the array. When only one element
 * is in the array, “in” behaves the same as “=”. 
 * 
 * The attribute name can be the name of a user defined attribute or one 
 * of the system defined attributes: 
 *	“name” : object key
 *	“size” : the object length
 *	“last-modified” : last modified date
 *	“content_type” : content type
 *	“bucket” : the bucket name 
 *	“owner”: the owner of the object
 * @param[out] rule - the output compiled rule
 * @throws api_exception in case of validation error.
 */
void
scope_compiler::compile(const Json::Value &scope, scope_rule &rule)
{
	compile(scope, rule, NULL);
}

/*
 * Compile a predicate node
 */
void
scope_compiler::compile_predicate(const Json::Value &scope,
    scope_rule &rule, logical_node *parent)
{
	string op, attr;
	oapi_attr_type attr_type = OAT_STRING;

	// allow empty scope: no conditions
	if (scope.isNull() || (scope.isArray() && scope.size() == 0))
		return;

	COMPARE_OPERATOR co = CO_INVALID;
	bool found = json_to_str(op, scope, QUERY_OPERATOR, false);
	if (!found) {
		throw api_exception(AEC_BAD_REQUEST,
		    "A query must be either a logical operator "
		    "or a predicate. Invalid scope: '%s'",
		    scope.toStyledString().c_str());
	}

	if (op == QOS_EQ)
		co = CO_EQUAL;
	else if (op == QOS_NOT_EQ)
		co = CO_NOTEQUAL;
	else if (op == QOS_GREATER)
		co = CO_GREATER;
	else if (op == QOS_GREATER_EQ)
		co = CO_GREATER_EQ;
	else if (op == QOS_LESS)
		co = CO_LESS;
	else if (op == QOS_LESS_EQ)
		co = CO_LESS_EQ;
	else if (op == QOS_LIKE)
		co = CO_LIKE;
	else if (op == QOS_IN)
		co = CO_IN;
	else {
		throw api_exception(AEC_BAD_REQUEST,
		    "Unsupported operator '%s'.", op.c_str());
	}

	found = json_to_str(attr, scope, QUERY_ATTR, false);
	if (!found || attr.empty()) {
		throw api_exception(AEC_BAD_REQUEST,
		    "A predicate must have an attr field.");
	}
	
	if (query_util::is_sys_attr(attr)) {
		if (!attr_mgr_.is_valid_attr(attr)) {
			throw api_exception(AEC_BAD_REQUEST,
			    "The attribute '%s' is not a valid system "
			    "attribute.",
			    attr.c_str());
		}

		const attr_def *def = attr_mgr_.get_attr_def(attr);
		attr_type = def->val_type_;

		if (attr == ISI_CONTENT_TYPE)
			ref_uda_ = true;
		else if ( attr != ISI_NAME && attr != ISI_ID && 
		    attr != ISI_OBJECT_TYPE)
			ref_stat_ = true;
	}
	else
		ref_uda_ = true;

	const Json::Value &value = scope[QUERY_VALUE];

	if (value.isNull()) {
		throw api_exception(AEC_BAD_REQUEST,
		    "A predicate must have a value field.");
	}

	if (co == CO_LIKE && (!value.isString() ||
	    value.asString().empty())) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Like operator must have a non-empty value.");
	}

	if (co == CO_IN) {
		if (!value.isArray()) {
			throw api_exception(AEC_BAD_REQUEST,
			    "An in operator must have an array of values.");
		}

		Json::Value::const_iterator it;
		// initialize it to value
		// we may need to allocate an different array
		Json::Value *p_value = const_cast<Json::Value *>(&value);

		for (it = value.begin(); it != value.end(); ++it) {
			const Json::Value & item = *it;

			if (attr_type == OAT_INT) {
				if (!item.isInt() &&
				    !item.isUInt()) {
					throw api_exception(AEC_BAD_REQUEST,
					    "Expect integer element for "
					    "attribute '%s'.",
					    attr.c_str());
				}
			}
			else if (!item.isString() ||
			    item.asString().empty()) {
				throw api_exception(AEC_BAD_REQUEST,
				    "In list must be all strings for attribute"
				    " '%s'.", attr.c_str());
			}

			if (attr_type == OAT_DATE) {
				if (p_value == &value) {
					p_value = new Json::Value(
					    Json::arrayValue);
					ASSERT(p_value);
				}

				// expect the value to be HTTP date
				time_t mt = 0;
				if (!convert_http_date_to_time(item.asString(),
				    mt)) {
					throw api_exception(AEC_BAD_REQUEST,
					    "'%s' must be a valid HTTP date. "
					    "'%s' is invalid.",
					    attr.c_str(),
					    item.asString().c_str());
				}
				Json::Value t_val = mt;
				p_value->append(t_val);
			}
			else if (query_util::is_owner_attr(attr)) {

				if (p_value == &value) {
					p_value = new Json::Value(
					    Json::arrayValue);
					ASSERT(p_value);
				}

				uid_t uid = -1;
				if (!get_uid_for_user(item.asString().c_str(),
				    uid)) {
					throw api_exception(AEC_BAD_REQUEST,
					    "'%s' is not a valid user.",
					    item.asString().c_str());
				}
				Json::Value t_val = uid;
				p_value->append(t_val);
			}
			else if (query_util::is_group_attr(attr)) {

				if (p_value == &value) {
					p_value = new Json::Value(
					    Json::arrayValue);
					ASSERT(p_value);
				}

				gid_t gid = -1;
				if (!get_gid_for_group(item.asString().c_str(),
				    gid)) {
					throw api_exception(AEC_BAD_REQUEST,
					    "'%s' is not a valid group.",
					    item.asString().c_str());
				}
				Json::Value t_val = gid;
				p_value->append(t_val);
			}
		}

		rule.add_predicate(attr, co, *p_value, parent);
		// clean p_value if we allocate it.
		if (p_value != &value) {
			delete p_value;
			p_value = NULL;
		}
	}
	else if (query_util::is_date_attr(attr)) {
		if (!value.isString() ||
		    value.asString().empty()) {
			throw api_exception(AEC_BAD_REQUEST,
			    "The value must be valid date string for attribute"
			    " '%s'.", attr.c_str());
		}

		if (co == CO_LIKE) {
			throw api_exception(AEC_BAD_REQUEST,
			    "'like' is not supported for"
			    " attribute '%s'.",
			    attr.c_str());
		}

		// expect the value to be HTTP date
		time_t mt = 0;
		if (!convert_http_date_to_time(value.asString(), mt)) {
				throw api_exception(AEC_BAD_REQUEST,
					"'%s' must be a valid HTTP date. "
					"'%s' is invalid.",
					attr.c_str(), value.asString().c_str());
		}
		Json::Value val = mt; // set value to date type
		rule.add_predicate(attr, co, val, parent);
	}
	else if (query_util::is_owner_attr(attr)) {
		if (!value.isString() ||
		    value.asString().empty()) {
			throw api_exception(AEC_BAD_REQUEST,
			    "The value must be a string for attribute"
			    " '%s'.", attr.c_str());
		}

		if (co != CO_EQUAL && co != CO_NOTEQUAL) {
			throw api_exception(AEC_BAD_REQUEST,
			    "Only =, != and in is supported for"
			    " attribute '%s'.",
			    attr.c_str());
		}

		uid_t uid = -1;
		if (!get_uid_for_user(value.asString().c_str(), uid)) {
			throw api_exception(AEC_BAD_REQUEST,
			    "'%s' is not a valid user.",
			    value.asString().c_str());
		}

		Json::Value val = uid;
		rule.add_predicate(attr, co, val, parent);
	}
	else if (query_util::is_group_attr(attr)) {
		if (!value.isString() ||
		    value.asString().empty()) {
			throw api_exception(AEC_BAD_REQUEST,
			    "The value must be a string for attribute"
			    " '%s'.", attr.c_str());
		}

		if (co != CO_EQUAL && co != CO_NOTEQUAL) {
			throw api_exception(AEC_BAD_REQUEST,
			    "Only =, != and in is supported for"
			    " attribute '%s'.",
			    attr.c_str());
		}

		gid_t gid = -1;
		if (!get_gid_for_group(value.asString().c_str(), gid)) {
			throw api_exception(AEC_BAD_REQUEST,
			    "'%s' is not a valid group.",
			    value.asString().c_str());
		}

		Json::Value val = gid;
		rule.add_predicate(attr, co, val, parent);
	}
	else if (attr == ISI_SIZE) {
		if (co == CO_LIKE) {
			throw api_exception(AEC_BAD_REQUEST,
			    "'like' is not supported for"
			    " attribute '%s'.",
			    attr.c_str());
		}

		if (!value.isInt() && !value.isUInt()) {
			throw api_exception(AEC_BAD_REQUEST,
			    "The value must be an integer for attribute"
			    " '%s'.", attr.c_str());
		}
		rule.add_predicate(attr, co, value, parent);
	}
	else {
		rule.add_predicate(attr, co, value, parent);
	}

	if (query_)
		query_->use_attr_in_scope(attr);

}

/*
 * Compile a logical node
 */
void
scope_compiler::compile_logical(
    const std::string &logical,
    const Json::Value &scope,
    scope_rule &rule, logical_node *parent)
{
	const Json::Value &conds = scope[QUERY_CONDITIONS];

	if (conds.isNull() || !conds.isArray() ||
	    conds.size() == 0) {
		throw api_exception(AEC_BAD_REQUEST,
		    "There must be a conditions array "
		    "in a query.");
	}

	LOGICAL_OPERATOR lo = LO_INVALID;
	if (logical == QLS_AND)
		lo = LO_AND;
	else if (logical == QLS_OR)
		lo = LO_OR;
	else if (logical == QLS_NOT)
		lo = LO_NOT;

	if (lo == LO_INVALID) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid logical operator:"
		    " '%s'", logical.c_str());
	}

	if (lo == LO_NOT && conds.size() != 1) {
		throw api_exception(AEC_BAD_REQUEST,
		    "There must be only one element in the conditions"
		    " array for 'not' operator.");
	}

	logical_node *ln = rule.add_logical(lo, parent);
	Json::Value::const_iterator it;

	for (it = conds.begin(); it != conds.end(); ++it) {
		const Json::Value &cond = *it;
		compile(cond, rule, ln);
	}
}

/*
 * Internal function compiling the the JSON rule into binary form.
 */
void
scope_compiler::compile(const Json::Value &scope, scope_rule &rule, 
    logical_node *parent)
{
	string logical;
	if (!scope.isNull() && !scope.isObject()) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid scope query: '%s'.",
		    scope.toStyledString().c_str());
	}

	bool is_logic = json_to_str(logical, scope, QUERY_LOGICAL, false);
	
	if (!is_logic) {
		// not a logical operator, it must be a predicate
		// which must have a compare operator
		compile_predicate(scope, rule, parent);
	}
	else
		compile_logical(logical, scope, rule, parent);
}

void
object_query::initialize(const Json::Value &query)
{
	const Json::Value *scope = NULL;
	const Json::Value &t_scp = query[QUERY_SCOPE];
	const Json::Value *rslt = NULL;
	const static Json::Value null_value;
	if (!t_scp.isNull()) {
		scope = &t_scp;
		rslt = &query[QUERY_RESULT];
	}
	else {
		rslt = &query[QUERY_RESULT];
		if (rslt->isNull())
			scope = &query;
		else
			scope = &null_value;
	}

	scope_compiler compiler(attr_mgr_, this);

	compiler.compile(*scope, scope_);

	if (!rslt || rslt->isNull()) {
		// result not specified, show name by default
		select_attr(ISI_NAME);
	} else {
		if (!rslt->isArray()) {
			throw api_exception(AEC_BAD_REQUEST,
			    "The result must be an array of strings.");
		}
		Json::Value::const_iterator it;
		for (it = rslt->begin(); it != rslt->end(); ++it) {
			const Json::Value & item = *it;
			if (!item.isString() ||
			    item.asString().empty()) {
				throw api_exception(AEC_BAD_REQUEST,
				    "In result must be all strings.");
			}
			const std::string & attr_name = item.asString();
			select_attr(attr_name);
		}
	}

	if (compiler.ref_stat())
		load_stat_ = true;
	if (compiler.ref_uda())
		load_uda_ = true;

	if (load_object_type_set_) {
		scope_.add_predicate(ISI_OBJECT_TYPE, CO_EQUAL,
		    object_type_);
		load_stat_ = true;
		use_attr_in_scope(ISI_OBJECT_TYPE);
	}
		
	if (load_hidden_set_) {
		scope_.add_predicate(ISI_HIDDEN, CO_EQUAL,
		    load_hidden_);
		use_attr_in_scope(ISI_HIDDEN);
	}
}

bool
object_query::is_attr_selected(const std::string &name) const
{
	std::set<std::string>::const_iterator it;
	it = result_.find(name);

	return it != result_.end();
}

bool
object_query::is_attr_sorted(const std::string &name) const
{
	std::set<std::string>::const_iterator it;
	it = sort_flds_.find(name);
	return it != sort_flds_.end();
}

void
object_query::check_attr(const std::string &name)
{
	if (query_util::is_sys_attr(name)) {
		if (!attr_mgr_.is_valid_attr(name)) {
			throw api_exception(AEC_BAD_REQUEST,
			    "The attribute '%s' is not a valid system "
			    "attribute.",
			    name.c_str());
		}

		if (name == ISI_CONTENT_TYPE)
			load_uda_ = true;
		else if (name != ISI_NAME && name != ISI_ID && 
		    name != ISI_OBJECT_TYPE)
			load_stat_ = true;
	}
	else
		load_uda_ = true;
}

void
object_query::select_attr(const std::string &name)
{
	check_attr(name);
	result_.insert(name);
}

void
object_query::add_sort_field(const sort_field &sort_field)
{
	check_attr(sort_field.field_);
	sort_info_.push_back(sort_field);
	sort_flds_.insert(sort_field.field_);
}

/*
 * mark the attribute used in scope query
 */
void
object_query::use_attr_in_scope(const std::string &name)
{
	scope_attrs_.insert(name);
}

/*
 * indicate if the attribute is used in scope query
 */
bool
object_query::is_attr_in_scope(const std::string &name) const
{
	std::set<std::string>::const_iterator it;
	it = scope_attrs_.find(name);
	return it != scope_attrs_.end();
}

bool
object_query::is_attr_used(const std::string &name) const
{
	return is_attr_selected(name) ||
	    is_attr_sorted(name) ||
	    is_attr_in_scope(name);
}

bool
dtc_bucket::is_applicable(const std::string &attr) const
{
	return !attr.compare(0, sizeof(ISI_BUCKET)-1, ISI_BUCKET);

}

/**
 * Check if the attribute is one of the system attribute
 */
bool
query_util::is_sys_attr(const std::string &attr_name)
{
	if (!strncasecmp(attr_name.c_str(),
	    EATTR_PREFIX,
	    sizeof(EATTR_PREFIX) - 1))
		return false;
	return true;
}

/**
 * Check if the attribute is date attribute
 */
bool
query_util::is_date_attr(const std::string &attr_name)
{
	if (attr_name == ISI_MTIME ||
	    attr_name == ISI_ATIME ||
	    attr_name == ISI_BTIME ||
	    attr_name == ISI_CTIME)
		return true;
	return false;
}

/**
 * Check if the attribute is owner attribute
 */
bool
query_util::is_owner_attr(const std::string &attr_name)
{
	if (attr_name == ISI_OWNER)
		return true;
	return false;
}

/**
 * Check if the attribute is group attribute
 */
bool
query_util::is_group_attr(const std::string &attr_name)
{
	if (attr_name == ISI_GROUP)
		return true;
	return false;
}


/** return true for 'hidden' entries */
bool
query_util::is_hidden(const char *p)
{
	return p && p[0] == '.';
}

std::string
query_util::get_mode_octal_str(const struct stat *pstat)
{
	ASSERT(pstat);
	char buf_mode[10];
	sprintf(buf_mode, "%04o", pstat->st_mode & (S_ISUID |
	    S_ISGID | S_ISVTX | S_IRWXU | S_IRWXG | S_IRWXO));
	return std::string(buf_mode);
}

/**
 * Get the user name from the UID
 */
void
query_util::get_user_name_from_uid(std::string &user_name, uid_t uid)
{
	persona *per = persona_alloc_uid(uid);
	char *name = NULL;
	int type = 0;

	if (per) {
		name = persona_to_name_alloc(per, &type);
		if (name) {
			user_name = name;
			free(name);
		}
		else
			user_name = UNKNOWN_USER;
		persona_free(per);

	}
	else
		user_name = UNKNOWN_USER;
}

/**
 * Get the group name from the GID
 */
void
query_util::get_group_name_from_gid(std::string &group_name, gid_t gid)
{
	persona *per = persona_alloc_gid(gid);
	char *name = NULL;
	int type = 0;

	if (per) {
		name = persona_to_name_alloc(per, &type);
		if (name) {
			group_name = name;
			free(name);
		}
		else
			group_name = UNKNOWN_GROUP;
		persona_free(per);
	}
	else
		group_name = UNKNOWN_GROUP;
}
