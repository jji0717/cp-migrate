#ifndef __OBJECT_QUERY_H__
#define __OBJECT_QUERY_H__

#include <isi_json/json_helpers.h>
#include <isi_object/ostore.h>
#include <isi_rest_server/api_utils.h>
#include <map>
#include <string>
#include <list>
#include <set>

#include "attribute_manager.h"

/**
 * Contains classes supporting object query.
 */

using std::map;
using std::string;

// base class disabling object copy
class nocopy {
public:
	nocopy() {}
private:
	nocopy(const nocopy &other);
	nocopy &operator = (const nocopy &other);
};

// generic class
class idt_class {
protected:
	idt_class() {}
public:
	virtual bool is_applicable(const string &attr) const { return true; }

	// return the single instance
	static const idt_class& instance() {
		static idt_class cl;
		return cl;
	}
};

/**
 * Interface for data transfer object
 */
class idt_object {

public:
	/**
	 * Get the given attribute value
	 * @param[in] key - the input key
	 * @param[out] value - the output value
	 * @return true if the key is found, false otherwise
	 */
	virtual bool get_attr(const string &key, Json::Value &value) const = 0;

	/**
	 * Get the given attribute value
	 * @param[in] key - the input key
	 * @return the pointer to the value if the key is found, NULL otherwise
	 */
	virtual const Json::Value *get_attr(const string &key) const = 0;

	/**
	 * set the given attribute value
	 * @param[in] key - the input key
	 * @param[in] value - the input value
	 */
	virtual void set_attr(const string &key, const Json::Value &value) = 0;

	/**
	 * clear the data transfer object
	 */
	virtual void clear() = 0;

	/**
	 * check if the attribute is applicable to the object
	 * (it can be bucket or object)
	 */
	virtual bool is_applicable(const string &attr) const
	{
		return class_.is_applicable(attr);
	}

	/**
	 * initialize the class
	 */
	explicit idt_object(const idt_class &iclass) : class_(iclass) {}

private:
	idt_class class_;
};

/**
 * Common data transfer object
 * Mt-unsafe: access to the same instance from multiple thread is unsafe.
 */
class dt_object : public nocopy, public idt_object{
	
public:
	/**
	 * Get the given attribute value
	 * @param[in] key - the input key
	 * @param[out] value - the output value
	 * @return true if the key is found, false otherwise
	 */
	virtual bool get_attr(const string &key, Json::Value &value) const;

	/**
	 * Get the given attribute value
	 * @param[in] key - the input key
	 * @return the pointer to the value if the key is found, NULL otherwise
	 */
	virtual const Json::Value *get_attr(const string &key) const;

	/**
	 * set the given attribute value
	 * @param[in] key - the input key
	 * @param[in] value - the input value
	 */
	virtual void set_attr(const string &key, const Json::Value &value);

	/**
	 * clear the data transfer object
	 */
	virtual void clear() { attr_map_.clear(); }
	
	/**
	 * constructor
	 */
	dt_object(const idt_class &iclass) : idt_object(iclass) {}
protected:
	/** 
	 * Property map.
	 * The key is the property key
	 * The value is the property value
	 */
	map<const string, Json::Value> attr_map_;
};

// bucket class
class dtc_bucket : public idt_class {
private:
	dtc_bucket() {}

public:
	virtual bool is_applicable(const string &attr) const;
	// return the single instance
	static dtc_bucket& instance() {
		static dtc_bucket cl;
		return cl;
	}
};

/**
 * data transfer object by composing a Json object directly.
 * This is useful for reusing the Json object when outputing.
 * Mt-unsafe: access to the same instance from multiple thread is unsafe.
 */
class jdt_object : public idt_object{

public:
	/**
	 * Get the given attribute value
	 * @param[in] key - the input key
	 * @param[out] value - the output value
	 * @return true if the key is found, false otherwise
	 */
	virtual bool get_attr(const string &key, Json::Value &value) const;

	/**
	 * Get the given attribute value
	 * @param[in] key - the input key
	 * @return the pointer to the value if the key is found, NULL otherwise
	 */
	virtual const Json::Value *get_attr(const string &key) const;

	/**
	 * set the given attribute value
	 * @param[in] key - the input key
	 * @param[in] value - the input value
	 */
	virtual void set_attr(const string &key, const Json::Value &value);

	/**
	 * clear the data transfer object
	 */
	virtual void clear() { attr_map_.clear(); }

	// access the json object directly
	Json::Value &get_json() { return attr_map_; }

	// access the json object directly
	const Json::Value &get_json() const { return attr_map_; }

	/**
	 * constructor
	 */
	jdt_object(const idt_class &iclass) : idt_object(iclass) {}

protected:
	
	
	Json::Value attr_map_;
private:
	jdt_object(const jdt_object&);
	jdt_object & operator=(const jdt_object&);
};


/**
 * Logical operator types
 */
enum LOGICAL_OPERATOR {
	LO_INVALID	= 0, // invalid
	LO_AND		= 1, // and
	LO_OR		= 2, // or
	LO_NOT		= 3  // not
};

/**
 * Comparison operator types
 */
enum COMPARE_OPERATOR {
	CO_INVALID	= 0, // invalid
	CO_EQUAL 	= 1, // =
	CO_NOTEQUAL 	= 2, // !=
	CO_GREATER 	= 3, // >
	CO_GREATER_EQ 	= 4, // >=
	CO_LESS 	= 5, // <
	CO_LESS_EQ 	= 6, // <=
	CO_LIKE 	= 7, // like
	CO_IN		= 8  // in
};

/**
 * Rule node type
 */
enum RULE_NODE_TYPE {
	RNT_LOGIC 	= 1,
	RNT_PREDICATE 	= 2
};

class logical_node;

/**
 * Model a generic rule node
 * Mt-unsafe: access to the same instance from multiple thread is unsafe.
 */
class rule_node : public nocopy {
protected:	
	RULE_NODE_TYPE node_type_;
	logical_node *parent_;
	
public:
	rule_node(RULE_NODE_TYPE ntype, logical_node *parent) :
	    node_type_(ntype), parent_(parent) {}

	RULE_NODE_TYPE node_type() const { return node_type_; }  
	virtual bool evaluate(const idt_object &object) const = 0;
	
friend class scope_rule;
friend class logical_node;

protected:	
	virtual ~rule_node() {}
	
};

/**
 * Model a logical rule node
 * Mt-unsafe: access to the same instance from multiple thread is unsafe.
 */
class logical_node : public rule_node {
public:
	logical_node(LOGICAL_OPERATOR op, logical_node *parent) :
	    rule_node(RNT_LOGIC, parent), logic_op_(op) {}
	
	void add_child(rule_node *child);
	virtual bool evaluate(const idt_object &object) const;

	LOGICAL_OPERATOR logic() const { return logic_op_; }
private:
	// children nodes, listed from top-down.
	std::list<rule_node *> children_;
	LOGICAL_OPERATOR logic_op_;
	
protected:	
	virtual ~logical_node();	
};

/**
 * Model a predicate node
 * Mt-unsafe: access to the same instance from multiple thread is unsafe.
 */
class predicate_node : public rule_node {
public:
	predicate_node(const string &attr,
	    COMPARE_OPERATOR co,
	    const Json::Value &value,
	    logical_node *parent) :
	    rule_node(RNT_PREDICATE, parent), attr_(attr),
	    comp_op_(co), value_(value) {}

	virtual bool evaluate(const idt_object &object) const;
private:
	string attr_;
	COMPARE_OPERATOR comp_op_;
	Json::Value value_;
};

/**
 * Class modeling the rule.
 * Mt-unsafe: access to the same instance from multiple thread is unsafe.
 */
class scope_rule : public nocopy {
public:	
	/**
	 * Evaluate the properties on an object
	 * and check if it satisfies the scope rule.
	 * @return true if it satisfies the rule, false otherwise
	 */
	bool evaluate(const idt_object &obj) const;
		
	/**
	 * ctor
	 */
	scope_rule() : root_(NULL) {}
	
	/**
	 * destructor:
	 */
	virtual ~scope_rule();
	
	/**
	 * cleanup
	 */
	virtual void cleanup();

	/**
	 * Public function to add an "And predicate" into the Root
	 * If the root is already an And logic, just add child predicate.
	 * If the root is or logic or a regular predicate then,
	 * create a new AND logic root and add the the new predicate and the
	 * old root as the children of the new root.
	 *
	 * This routine is useful only in the case, the system wants to piggy-
	 * back ride the JSON query.
	 */
	void add_predicate(const string &attr,
	    COMPARE_OPERATOR co,
	    const Json::Value &value);

friend class scope_compiler;
private:
	/**
	 * Add a logical node, 
	 * @param[in] op - the logical operator
	 * @param[in] parent - the parent logical node. If the root node, 
	 *                     parent should be NULL
	 * @return the newly added node, note the pointer ownership
	 * belongs to scope_rule, the caller should not delete it
	 */
	virtual logical_node *add_logical(LOGICAL_OPERATOR op, 
	    logical_node *parent);
	
	/**
	 * Add a predicate node.
	 * @param[in] attr - the attribute name
	 * @param[in] co - the comparison operator
	 * @value[in] value - the Json value for operand
	 * @param[in] op - the logical operator
	 * @param[in] parent - the parent logical node. If the root node, 
	 *                     parent should be NULL
	 * @return the newly added node, note the pointer ownership
	 * belongs to scope_rule, the caller should not delete it
	 */	
	predicate_node *add_predicate(const string &attr,
	    COMPARE_OPERATOR co,
	    const Json::Value &value,
	    logical_node *parent);
	
	// root rule node:
	rule_node *root_;
};

class object_query;

/**
 * Class for compiling JSON scope specification into a rule tree
 * Mt-unsafe: access to the same instance from multiple thread is unsafe.
 */
class scope_compiler : public nocopy {
public:

	/**
	 *
	 */
	scope_compiler(const attribute_manager &attr_mgr,
	    object_query *query = NULL)
	    : ref_stat_(false)
	    , ref_uda_(false), attr_mgr_(attr_mgr), query_(query) {}

	/**
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
	 *	“CONTENT_TYPE” : content type 
	 *	“bucket” : the bucket name 
	 *	“owner”: the owner of the object
	 * @param[out] rule - the output compiled rule
	 * @throws api_exception in case of validation error.
	 */
	
	void compile(const Json::Value &scope, scope_rule &rule);

	bool ref_stat() { return ref_stat_; }
	bool ref_uda() { return ref_uda_; }
private:
	/**
	 * Internal function compiling the the JSON rule into binary form.
	 * @param[in] scope - the current JSon rule node under processing
	 * @param[in/out] rule - the complied rule
	 * @param[in/out] parent - the parent logical rule node
	 * @throws api_exception in case of validation error.
	 */
	void compile(const Json::Value &scope, scope_rule &rule, 
	    logical_node *parent);	

	/**
	 * Internal function to compile a predicate node
	 * @param[in] scope - the current JSon rule node under processing
	 * @param[in/out] rule - the complied rule
	 * @param[in/out] parent - the parent logical rule node
	 * @throws api_exception in case of validation error.
	 */
	void compile_predicate(const Json::Value &scope, scope_rule &rule,
	    logical_node *parent);

	/**
	 * Internal function to compile a logical node
	 * @param[in] logical - the logical operator string
	 * @param[in] scope - the current JSon rule node under processing
	 * @param[in/out] rule - the complied rule
	 * @param[in/out] parent - the parent logical rule node
	 * @throws api_exception in case of validation error.
	 */
	void compile_logical(const std::string &logical,
	    const Json::Value &scope, scope_rule &rule,
	    logical_node *parent);

	/**
	 * Indicate if additional system attributes are referenced
	 */
	bool ref_stat_;

	/**
	 * Indicate if user defined attributes are referenced
	 */
	bool ref_uda_;

	/**
	 * The attribute manager
	 */
	const attribute_manager &attr_mgr_;

	object_query *query_;

};

/**
 * Specify the sort field.
 */
struct sort_field {
	std::string field_;
	sort_dir dir_;
};


/**
 * Class modeling an object query. There are two components:
 * 1. Selection list
 * 2. Scope
 */
class object_query : public nocopy {
public:
	/**
	 * given the Json query value, initialize the object query
	 * throws exception in case of validation errors
	 * The query can be a scope_query or a full query
	 * that consists of a result specification and a scope specification
	 * {
	 * 	"result" : [<attributes>]
	 * 	"scope" : <scope_query>
	 * }
	 */
	void initialize(const Json::Value &query);

	object_query(const attribute_manager &attr_mgr) :
	    load_stat_(false),
	    load_uda_(false),
	    max_keys_(-1), load_hidden_set_(false),
	    load_object_type_set_(false),
	    attr_mgr_(attr_mgr) { }

	// accessors:

	/**
	 * if to load additional system attributes
	 */
	bool load_stat() const { return load_stat_; }

	/**
	 * if to load user-defined attributes
	 */
	bool load_uda() const { return load_uda_; }

	/**
	 * Return the reference to the scope rule
	 */
	const scope_rule &rule() const { return scope_; }

	/**
	 * Check if the specified name is in the result list
	 */
	bool is_attr_selected(const std::string &name) const;

	/**
	 * Check if the specified name is in the sort list
	 */
	bool is_attr_sorted(const std::string &name) const;

	/**
	 * select the specified attribute
	 */
	void select_attr(const std::string &name);

	/**
	 * mark the attribute used in scope query
	 */
	void use_attr_in_scope(const std::string &name);

	/**
	 * indicate if the attribute is used in scope query
	 */
	bool is_attr_in_scope(const std::string &name) const;

	/**
	 * Indicate if an attribute is used in anyway in the query:
	 * in sorting, selection list or predicates.
	 */
	bool is_attr_used(const std::string &name) const;

	/**
	 * Get the sorting information.
	 */
	const std::list<sort_field> &get_sort_info() const
	{
		return sort_info_;
	}

	/**
	 * Add a sort field to the query.
	 */
	void add_sort_field(const sort_field &sort_field);

	/**
	 * Indicates if the sort is requested
	 */
	bool to_sort() const { return !sort_info_.empty(); }


	size_t get_max_keys() const { return max_keys_; }
	void set_max_keys(size_t max_keys)
	{
		max_keys_ = max_keys;
	}

	bool to_load_hidden() const { return load_hidden_; }
	void set_load_hidden(bool load_hidden)
	{
		load_hidden_ = load_hidden;
		load_hidden_set_ = true;
	}

	const std::string &get_load_object_type() const { return object_type_; }
	void set_load_object_type(const char *object_type)
	{
		object_type_ = object_type;
		load_object_type_set_ = true;
	}

	const attribute_manager &get_attr_mgr() const { return attr_mgr_; }

private:
	/**
	 * Check if the given attribute is valid.
	 * @throws exception when invalid
	 */
	void check_attr(const std::string &attr);
	std::list<sort_field> sort_info_;
	std::set<std::string> sort_flds_; // the sort fields
	std::set<std::string> result_; // for fast lookup
	std::set<std::string> scope_attrs_; // for fast lookup

	scope_rule scope_;

	/**
	 * Indicate if to load additional system attributes
	 */
	bool load_stat_;

	/**
	 * Indicate if to load user defined attributes
	 */
	bool load_uda_;

	size_t max_keys_;

	/**
	 * The following should be modeled as part of the scope predicates:
	 */
	bool load_hidden_;
	bool load_hidden_set_;

	/**
	 * The following should be modeled as part of the scope predicates:
	 */
	std::string object_type_;
	bool load_object_type_set_;

	const attribute_manager &attr_mgr_;
};

/**
 * Check if the attribute is one of the system attribute
 */
class query_util {
public:
	static bool is_sys_attr(const std::string &attr_name);
	static bool is_date_attr(const std::string &attr_name);
	static bool is_owner_attr(const std::string &attr_name);
	static bool is_group_attr(const std::string &attr_name);
	static bool is_hidden(const char *p);
	static std::string get_mode_octal_str(const struct stat *pstat);
	static void get_user_name_from_uid(std::string &user_name, uid_t uid);
	static void get_group_name_from_gid(std::string &group_name, gid_t gid);

};


#endif //__OBJECT_QUERY_H__
