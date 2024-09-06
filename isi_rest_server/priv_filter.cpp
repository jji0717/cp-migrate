#include <sys/isi_privilege.h>

#include <assert.h>
#include <unistd.h>

#include <isi_util_cpp/fmt_helpers.h>

#include "api_error.h"
#include "priv_filter.h"

const priv_filter priv_filter::empty_filter;

priv_filter::priv_filter()
{
	allow_ro_post_ = false;
	set_run_as(true);
}

priv_filter::priv_filter(int default_priv)
{
	assert(isi_priv_id_get_def(default_priv));

	allow_ro_post_ = false;
	privs_[request::GET].priv_list_.push_back(default_priv);
	privs_[request::PUT].priv_list_.push_back(default_priv);
	privs_[request::POST].priv_list_.push_back(default_priv);
	privs_[request::DELETE].priv_list_.push_back(default_priv);
	privs_[request::HEAD].priv_list_.push_back(default_priv);

	set_run_as(true);
}

priv_filter &
priv_filter::add_priv(int priv)
{
	assert(isi_priv_id_get_def(priv));

	privs_[request::GET].priv_list_.push_back(priv);
	privs_[request::PUT].priv_list_.push_back(priv);
	privs_[request::POST].priv_list_.push_back(priv);
	privs_[request::DELETE].priv_list_.push_back(priv);
	privs_[request::HEAD].priv_list_.push_back(priv);
	return *this;
}

priv_filter &
priv_filter::add_priv(int priv, request::method method)
{
	assert(isi_priv_id_get_def(priv));
	assert(method >= request::GET && method < request::NUM_METHODS);
	privs_[method].priv_list_.push_back(priv);
	return *this;
}

priv_filter &
priv_filter::require_all_privs()
{
	privs_[request::GET].oper_ = AND;
	privs_[request::PUT].oper_ = AND;
	privs_[request::POST].oper_ = AND;
	privs_[request::DELETE].oper_ = AND;
	privs_[request::HEAD].oper_ = AND;
	return *this;
}

priv_filter &
priv_filter::require_all_privs(request::method method)
{
	assert(method >= request::GET && method < request::NUM_METHODS);
	privs_[method].oper_ = AND;
	return *this;
}



priv_filter &
priv_filter::set_run_as(bool r)
{
	for (unsigned i = 0; i < request::NUM_METHODS; ++i)
		run_as_[i] = r;
	return *this;
}

priv_filter &
priv_filter::set_run_as(request::method m, bool r)
{
	assert(m >= 0 && m < request::NUM_METHODS);
	run_as_[m] = r;
	return *this;
}

// used when read-only privilege is sufficient for POST method
priv_filter &
priv_filter::allow_ro_post()
{
	allow_ro_post_ = true;
	return *this;
}

bool
priv_filter::check_priv(int fd, request::method method) const
{
	assert(method >= request::GET && method < request::NUM_METHODS);

	const list_operator op = privs_[method].oper_;
	const priv_list &list = privs_[method].priv_list_;

	// If there are no privs required, all checks pass
	if (list.empty()) {
		return true;
	}
	
	for (priv_list::const_iterator priv_iter = list.begin();
	    priv_iter != list.end(); priv_iter++) {

		if (check_single_priv(fd, *priv_iter, method)) {
			if (op == OR) {
				return true;
			}
		} else {
			if (op == AND) {
				return false;
			}
		}
	}

	return op == AND;
}

void
priv_filter::throw_unless_privileged(int fd, request::method m) const
{
	if (check_priv(fd, m))
		return;

	const list_operator op = privs_[m].oper_;
	const priv_list &pl = privs_[m].priv_list_;

	std::string msg = "Privilege check failed. ";

	const std::string t = (m == request::GET || m == request::HEAD ||
			       (allow_ro_post_ && m == request::POST))
	    ? " read "
	    : " write ";

	if (pl.size() == 1)
		msg += "The following" + t + "privilege is required: ";
	else if (op == AND)
		msg += "All of the following" + t + "privileges are required: ";
	else
		msg += "One of the following" + t + "privileges is required: ";

	for (priv_list::const_iterator it = pl.begin(); it != pl.end(); ++it) {
		if (it != pl.begin())
			msg += ", ";
		msg += isi_printf_to_str("%s (%{})",
		    isi_priv_id_get_name(*it), isi_priv_id_fmt(*it));
	}

	throw api_exception(AEC_FORBIDDEN, msg);
}

bool
priv_filter::check_single_priv(int fd, int priv, request::method method) const
{
	const struct isi_priv_def *p_def = isi_priv_id_get_def(priv);

	//some privileges only have a unary assignment (no read/write)
	// mode: only check for R/W priv assignment if the PRIV object
	// is configured to have one
	int flag = p_def->priv_attrs & ISI_PRIV_FLAG_READWRITE;

	// GET and HEAD only need read permissions, all other methods may
	// require read and write permissions (see above)
	if (method == request::GET || method == request::HEAD ||
	    (allow_ro_post_ && method == request::POST)) {
		flag = ISI_PRIV_FLAG_NONE;
	}

	return ipriv_check_cred(fd, priv, flag) == 0;
}
