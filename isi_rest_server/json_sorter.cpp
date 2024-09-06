#include "json_sorter.h"

#include "api_error.h"

#include <isi_ilog/ilog.h>
#include <boost/algorithm/string.hpp>


json_sorter::json_sorter(const std::string &primary_field, const field_set
    *fields, unsigned field_count, int dir, bool shared_only,
    bool ignore_strcase) :
	primary_field_(primary_field), dir_(dir), shared_only_(shared_only),
	ignore_strcase_(ignore_strcase)
{
	arg_validate_sort_dir(dir_);

	for (unsigned i = 0; i < field_count; i++) {
		if (fields[i].name != primary_field_) {
			field_sort_order_.push_back(fields[i].name);
		}
	}
}

json_sorter::json_sorter(const std::string& primary_field,
    const std::vector<std::string> &field_sort_order,
    int dir, bool shared_only, bool ignore_strcase) :
	primary_field_(primary_field), dir_(dir), shared_only_(shared_only),
	ignore_strcase_(ignore_strcase)
{
	arg_validate_sort_dir(dir_);

	for (std::vector<std::string>::const_iterator iter =
	    field_sort_order.begin(); iter != field_sort_order.end(); ++iter) {
		const std::string& name = *iter;
		if (name != primary_field_) {
			field_sort_order_.push_back(name);
		}
	}
}

json_sorter::json_sorter(const std::string& primary_field,
    const std::vector<std::string> &field_sort_order,
    const std::map<std::string, int> &field_sort_dirs,
    int dir, bool shared_only, bool ignore_strcase) :
	primary_field_(primary_field), dir_(dir), shared_only_(shared_only),
	ignore_strcase_(ignore_strcase)
{
	arg_validate_sort_dir(dir_);

	for (std::vector<std::string>::const_iterator iter =
	    field_sort_order.begin(); iter != field_sort_order.end();
	    ++iter) {
		const std::string& name = *iter;
		if (name != primary_field_) {
			field_sort_order_.push_back(name);
		}
	}

	for (std::map<std::string, int>::const_iterator iter =
	    field_sort_dirs.begin(); iter != field_sort_dirs.end(); ++iter) {
		const std::string& name = iter->first;
		if (name != primary_field_) {
			field_sort_dirs_[name] = iter->second;
		}
	}
}

bool
json_sorter::operator()(const Json::Value &a, const Json::Value &b) const
{
	arg_validate_sort_dir(dir_);

	int c = 0;
	const Json::Value *a_val = NULL;
	const Json::Value *b_val = NULL;
	int sort_dir = dir_;

	if (!shared_only_ || (a.isMember(primary_field_) &&
			      b.isMember(primary_field_))) {
		a_val = &a[primary_field_];
		b_val = &b[primary_field_];

		c = compare_member(*a_val, *b_val, ignore_strcase_);
	}

	for (std::vector<std::string>::const_iterator iter =
	    field_sort_order_.begin(); (c == 0) &&
	    (iter != field_sort_order_.end()); ++iter) {
		const std::string& name = *iter;

		std::map<std::string, int>::const_iterator dir_iter =
		    field_sort_dirs_.find(name);

		if (dir_iter != field_sort_dirs_.end()) {
			sort_dir = dir_iter->second;
		}

		if (!shared_only_ || (a.isMember(name) &&
				      b.isMember(name))) {
			a_val = &a[name];
			b_val = &b[name];

			c = compare_member(*a_val, *b_val, ignore_strcase_);
		}
	}

	if (sort_dir == SORT_DIR_ASC) {
		return c < 0;
	} else if (sort_dir == SORT_DIR_DESC) {
		return c > 0;
	} else {
		throw api_exception(AEC_BAD_REQUEST,
		    "Sort direction is invalid.");
	}
}

int
json_sorter::compare_member(const Json::Value &a, const Json::Value &b,
    bool ignore_strcase)
{
	/*
	 * We will not sort objects by members that are arrays or objects
	 * because these types can't easily be put in a resume token.
	 */
	if ((a.isArray() || a.isObject()) && (b.isArray() || b.isObject())) {
		return 0;
	}
	/*
	 * Workaround: Json::Value.compare() will not correctly compare signed
	 * and unsigned integers, but this compare_member() function is
	 * sometimes called to compare two unsigned Json ID values when JsonCPP
	 * has for some reason interpreted one of them as signed (for example
	 * in uri_handler::format_list_response() when sorting integer ids of
	 * some PAPI objects).  Convert both values to integers and compare
	 * manually in these cases.
	 */
	if ((a.type() == Json::intValue && b.type() == Json::uintValue) ||
	    (a.type() == Json::uintValue && b.type() == Json::intValue)) {
		int aInt = a.asInt();
		int bInt = b.asInt();
		if (aInt < bInt) {
			return -1;
		}
		if (aInt > bInt) {
			return 1;
		}
		return 0;
	}

	if (ignore_strcase && a.isString() && b.isString()) {
		/*
		 * NOTE: this functionality should eventually move inside
		 * jsoncpp as usecases increase. (ideally jsoncpp should be
		 * using strcasecmp for this)
		 */
		std::string a_str = a.asString();
		std::string b_str = b.asString();
		int ret;

		/* convert to lower case */
		boost::algorithm::to_lower(a_str);
		boost::algorithm::to_lower(b_str);
		/* replace the json values with new ones */
		const Json::Value &a = Json::Value(a_str);
		const Json::Value &b = Json::Value(b_str);
		if ((ret = a.compare(b)) != 0)
			return ret;
		/* otherwise use regular comparison to break the tie */
	}

	return a.compare(b);
}

