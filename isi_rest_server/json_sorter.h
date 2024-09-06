#ifndef __JSON_SORTER_H__
#define __JSON_SORTER_H__

#include <string>
#include <vector>

#include <jsoncpp/json.h>

#include "api_utils.h"

class json_sorter {
public:
	json_sorter(const std::string &primary_field, const field_set
	    *fields, unsigned field_count, int dir, bool shared_only=false,
	    bool ignore_strcase=false);

        json_sorter(const std::string& primary_field,
	    const std::vector<std::string> &field_sort_order,
	    int dir, bool shared_only=false, bool ignore_strcase=false);

	json_sorter(const std::string& primary_field,
	    const std::vector<std::string> &field_sort_order,
	    const std::map<std::string, int> &field_sort_dirs,
	    int dir, bool shared_only=false, bool ignore_strcase=false);

	bool operator() (const Json::Value &a, const Json::Value &b) const;

private:
	std::string primary_field_;
	std::vector<std::string> field_sort_order_;
	std::map<std::string, int> field_sort_dirs_;
	int dir_;
	bool shared_only_;
	bool ignore_strcase_;

	static int compare_member(const Json::Value &a, const Json::Value &b,
	    bool ignore_strcase = false);

};

#endif

