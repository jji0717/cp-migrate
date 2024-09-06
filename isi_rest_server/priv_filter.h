#ifndef __PRIV_FILTER_H__
#define __PRIV_FILTER_H__

#include <vector>

#include "request.h"

class priv_filter {
public:
	static const priv_filter empty_filter;

	priv_filter();

	explicit priv_filter(int default_priv);

	priv_filter &add_priv(int priv);
	priv_filter &add_priv(int priv, request::method method);

	priv_filter &require_all_privs();
	priv_filter &require_all_privs(request::method method);

	priv_filter &allow_ro_post();

	/** Set run-as-user for all methods */
	priv_filter &set_run_as(bool r);

	/** Set run-as-user for a specific method */
	priv_filter &set_run_as(request::method m, bool r);

	/** Return whether user @fd has privileges for method @m */
	bool check_priv(int fd, request::method m) const;

	/** Perform check_priv() and on failure throw forbidden exception
	 * with message explaining privilege requirements. */
	void throw_unless_privileged(int fd, request::method m) const;

	inline bool get_run_as(request::method m) const;

private:
	enum list_operator {
		OR = 0, AND
	};

	typedef std::vector<int> priv_list;

	struct method_priv {
		method_priv()
		    : oper_(OR)
		{ }

		list_operator oper_;
		priv_list     priv_list_;
	};

	method_priv privs_[request::NUM_METHODS];

	bool allow_ro_post_;
	bool run_as_[request::NUM_METHODS];

	bool check_single_priv(int fd, int priv, request::method m) const;
};

/*  _       _ _
 * (_)_ __ | (_)_ __   ___
 * | | '_ \| | | '_ \ / _ \
 * | | | | | | | | | |  __/
 * |_|_| |_|_|_|_| |_|\___|
 */

bool
priv_filter::get_run_as(request::method m) const
{
	return run_as_[m];
}

#endif
