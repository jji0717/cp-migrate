#include <iostream>
#include <fstream>

#include <boost/algorithm/string/replace.hpp>

#include <isi_ilog/ilog.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_util_cpp/uri_encode.h>
#include <isi_util_cpp/user_text.h>

#include "api_utils.h"
#include "uri_handler.h"
#include "uri_mapper.h"
#include "handler_doc.h"

namespace {

const char TOK_BEG           = '<';
const char TOK_END           = '>';
const char TOK_CHARS[]       = "<>";
const char TOK_SUFFIX_SPLAT  = '*';
const char TOK_SUFFIX_PLUS   = '+';
const char PATH_SEP          = '/';
const char DOUBLE_PATH_SEP[] = "//";

void
log_stats(const char *meth, const char *path, method_stats &ms, bool clear)
{
	if (ms.calls_ == 0)
		return;
	ilog(IL_INFO, "%s %s calls:%u errors:%u time:%lu",
	    meth, path, ms.calls_, ms.errors_, ms.time_);
	if (clear)
		ms.clear();
}

}

/**
 * patttern_handler
 */
uri_mapper::pattern_handler::pattern_handler(const std::string &rep,
    bool internal)
    : rep_(rep), internal_(internal)
{ }

uri_mapper::pattern_handler::pattern_handler(const std::string &rep,
    uri_handler *h, bool internal)
    : rep_(rep), internal_(internal)
{
	hmap_[rep] = h;
}

std::string
uri_mapper::pattern_handler::parse(const std::string &path, unsigned pfx_len)
{
	pattern                pat;
	std::string::size_type b = 0, e;

	if (path.find(DOUBLE_PATH_SEP) != std::string::npos)
		throw isi_exception(
		    get_user_text(REST_MALFORMED_PATH_x), path.c_str());

	/* determine prefix */
	for (unsigned i = 0; i < pfx_len; ++i) {
		if (b >= path.size() || path[b] != PATH_SEP ||
		    ++b == path.size())
			throw isi_exception(
			    get_user_text(REST_MALFORMED_PATH_x), path.c_str());

		b = path.find(PATH_SEP, b);
	}

	std::string prefix(path, 0, b);

	/* check for token chars in the prefix part, probably not wanted */
	if (prefix.find_first_of(TOK_CHARS) != std::string::npos)
		throw isi_exception(
		    get_user_text(REST_MALFORMED_PATH_x), path.c_str());

	pvec_.clear();

	while (b < path.size()) {
		if (path[b] != PATH_SEP)
			throw isi_exception(
			    get_user_text(REST_MALFORMED_PATH_x), path.c_str());

		++b; /* skip slash */

		/* literal? */
		if (path[b] != TOK_BEG) {
			e = path.find(PATH_SEP, b);

			pat.type_ = pattern::LITERAL;
			pat.name_ = path.substr(b, e - b);
			pvec_.push_back(pat);

			b = e;
			continue;
		}

		/* else token, skip < and find end */
		if ((e = path.find(TOK_END, ++b)) == std::string::npos)
			throw isi_exception(
			    get_user_text(REST_MALFORMED_PATH_x), path.c_str());

		if (e - b == 0)
			throw isi_exception(
			    get_user_text(REST_MALFORMED_PATH_x), path.c_str());

		if (e - b > 2 && path[e - 1] == TOK_SUFFIX_SPLAT) {
			pat.name_ = path.substr(b, e - b - 1);
			pat.type_ = pattern::ZERO_OR_MORE;
		} else if (e - b > 2 && path[e - 1] == TOK_SUFFIX_PLUS) {
			pat.name_ = path.substr(b, e - b - 1);
			pat.type_ = pattern::ONE_OR_MORE;
		} else {
			pat.name_ = path.substr(b, e - b);
			pat.type_ = pattern::SINGLE;
		}

		/* N.B. uris are probably small enough for this to be OK */
		for (unsigned i = 0; i < pvec_.size(); ++i)
			if (pvec_[i].type_ != pattern::LITERAL &&
			    pvec_[i].name_ == pat.name_)
				throw isi_exception(
				    get_user_text(REST_DUPLICATE_TOKEN_NAME_x),
					path.c_str());

		pvec_.push_back(pat);

		b = e + 1;
	}

	/* greedy matchers must be last component */
	for (unsigned i = 0; i + 1 < pvec_.size(); ++i)
		if (pvec_[i].type_ == pattern::ZERO_OR_MORE ||
		    pvec_[i].type_ == pattern::ONE_OR_MORE)
			throw isi_exception(
			    get_user_text(REST_MALFORMED_PATH_x), path.c_str());

	return prefix;
}

bool
uri_mapper::pattern_handler::conflicts_with(const pattern_handler &x) const
{
	unsigned t_len = pvec_.size();
	unsigned x_len = x.pvec_.size();
	unsigned i;

	/* zero-length cases */
	if (t_len == 0 || x_len == 0)
		return t_len == x_len;

	for (i = 0; i < t_len && i < x_len; ++i) {
		/* ambiguity in the hierarchy */
		if (pvec_[i].type_ != x.pvec_[i].type_)
			return true;

		/* literal induced branching */
		if (pvec_[i].type_ == pattern::LITERAL &&
		    pvec_[i].name_ != x.pvec_[i].name_)
			return false;
	}

	/* completely equal? */
	if (t_len == x_len)
		return true;

	/* next (and last) component of longer path is greedy? */
	if (t_len > x_len) {
		if (pvec_[i].type_ == pattern::ZERO_OR_MORE)
			return true;
	} else
		if (x.pvec_[i].type_ == pattern::ZERO_OR_MORE)
			return true;

	return false;
}

bool
uri_mapper::pattern_handler::matches(const std::string &path,
    std::string::size_type p, uri_tokens &tokens) const
{
	std::string::size_type b = p, e;

	tokens.clear();

	std::string buf;

	/*
	 * Loop through pattern components, skip first component + /, redundant
	 * with prefix already checked
	 */
	for (pattern_vec::const_iterator it = pvec_.begin();
	    it != pvec_.end(); ++it) {
		/* greedy component can match empty tail string without slash */
		if (b >= path.size()) {
			if (it->type_ == pattern::ZERO_OR_MORE) {
				tokens[it->name_] = "";
				return true;
			} else {
				return false;
			}
		}

		/* check and skip slash, can fail after literal match */
		if (path[b] != PATH_SEP)
			return false;
		++b;

		switch (it->type_) {
		case pattern::LITERAL:
			if (path.compare(b, it->name_.size(), it->name_) != 0)
				return false;
			b += it->name_.size();
			break;

		case pattern::SINGLE:
			e = path.find(PATH_SEP, b);
			buf.assign(path, b, e - b);
			uri_comp_decode(buf);
			tokens[it->name_] = buf;
			b = e;
			break;

		case pattern::ZERO_OR_MORE:
		case pattern::ONE_OR_MORE:
			buf.assign(path, b, std::string::npos);
			uri_comp_decode(buf);
			tokens[it->name_] = buf;
			return true;
		}
	}

	return b >= path.size();
}

uri_mapper::pattern_handler::~pattern_handler()
{
	for (pattern_handler::iterator it = begin(); it != end(); ++it) {
		delete it->second;
	}
}

void
uri_mapper::pattern_handler::log_method_stats(bool clear) const
{
	for (const_iterator it = begin(); it != end(); ++it) {
		for (unsigned m = 0; m < request::NUM_METHODS + 1; ++m)	{
			if (it->second != NULL)
				log_stats(request::string_from_method(m),
				    it->first.c_str(), it->second->stats_[m],
				    clear);
		}
	}
}

/**
 * uri_mapper
 */
uri_mapper::uri_mapper(unsigned prefix_len)
	: prefix_len_(prefix_len)
{ }

uri_mapper::~uri_mapper()
{
	for (pattern_map::iterator it = patterns_.begin(); it !=
	    patterns_.end(); ++it) {
		delete it->second;
	}
}

void
uri_mapper::add(const std::string &path, uri_handler *h, bool internal)
{
	pattern_handler *phandler = new pattern_handler(path, h, internal);
	std::auto_ptr<pattern_handler> p(phandler);

	std::string prefix = phandler->parse(path, prefix_len_);

	/* find any duplicates */
	std::pair<pattern_map::const_iterator, pattern_map::const_iterator> r;
	r = patterns_.equal_range(prefix);
	for (pattern_map::const_iterator it = r.first; it != r.second; ++it)
		if (phandler->conflicts_with(*it->second))
			throw isi_exception(
			    get_user_text(REST_DUPLICATE_PATH_x), path.c_str());

	/* otherwise insert */
	pattern_map::iterator it;
	p.release();
	it = patterns_.insert(std::make_pair(prefix, phandler));
	directory_[path] = phandler;
}

void
uri_mapper::merge_all(uri_mapper &other)
{
	for (uri_mapper::iterator it = other.begin(); it != other.end(); ++it) {
		pattern_handler *ph = it->second;

		if (ph != NULL) {
			for (pattern_handler::iterator pit = ph->begin();
			    pit != ph->end(); ++pit) {
				std::string path = pit->first;
				uri_handler *h = pit->second;
				bool internal = ph->is_internal();

				add(path, h, internal);

				// Unmap the handler in the other map.
				pit->second = NULL;
			}
		}
	}
}

uri_handler *
uri_mapper::resolve(request &input, response &output) const
{
	return resolve(input.get_uri(), input.tokens());
}

uri_handler *
uri_mapper::resolve(const std::string &path, uri_tokens &tokens) const
{
	pattern_handler *ph = resolve_pattern(path, tokens);

	if (ph == NULL) {
		return NULL;
	} else if (ph->begin() != ph->end()) {
		return ph->begin()->second;
	} else {
		return NULL;
	}
}

uri_mapper::pattern_handler *
uri_mapper::resolve_pattern(const std::string &path, uri_tokens &tokens) const
{
	std::string::size_type p = 0;
	unsigned prefix_len = 0;

	/* find prefix */
	for ( ; prefix_len < prefix_len_; ++prefix_len) {
		if (p >= path.size() || path[p] != PATH_SEP ||
		    ++p == path.size())
			return NULL;

		p = path.find(PATH_SEP, p);
	}

	/* should match expected length in class */
	if (prefix_len != prefix_len_)
		return NULL;

	std::string prefix(path, 0, p);

	/* find first prefix pattern that matches */
	std::pair<pattern_map::const_iterator, pattern_map::const_iterator> r;
	r = patterns_.equal_range(prefix);
	for (pattern_map::const_iterator it = r.first; it != r.second; ++it) {
		if (it->second->matches(path, p, tokens)) {
			return it->second;
		}
	}
	tokens.clear(); // if no match
	return NULL;
}

void
uri_mapper::log_method_stats(bool clear) const
{
	for (const_iterator it = begin(); it != end(); ++it)
		it->second->log_method_stats(clear);

	log_stats("ALL", "UNKNOWN", unknown_stats_, clear);
	log_stats("GET", "DESCRIBE", describe_stats_, clear);
}

void
uri_mapper::get_uri_list(const std::string &prefix, bool internal,
    std::list<std::string> &ret) const
{
	ret.clear();

	directory_map::const_iterator lb;
	directory_map::const_iterator ub;

	if (prefix.empty())  {
		lb = directory_.begin();
		ub = directory_.end();
	} else {
		std::string lb_uri = prefix + '/';
		std::string ub_uri = prefix + char('/' + 1);

		lb = directory_.lower_bound(lb_uri);
		ub = directory_.upper_bound(ub_uri);
	}

	/*
	* Filter out internal unless that argument is given.
	*/
	for (directory_map::const_iterator it = lb; it != ub; ++it) {
		if (it->second != NULL) {
			if (internal || !it->second->is_internal()) {
				ret.push_back(it->first);
			}
		}
	}
}

void
uri_mapper::get_directory(const request &input, response &output,
    bool internal) const
{
	std::list<std::string> uris;
	get_uri_list(input.get_uri(), internal, uris);

	Json::Value &j_output = output.get_json();
	j_output["directory"] = Json::arrayValue;
	Json::Value &j_dir = j_output["directory"];

	for (std::list<std::string>::iterator it = uris.begin(); it !=
	    uris.end(); ++it) {
		j_dir.append(*it);
	}
}

bool
uri_mapper::handle_metadata_requests(request &input, response &output) const
{
	return false;
}

/**
 * version_handler
 */
versioned_mapper::version_handler::version_handler(const std::string &v) :
    version_str_(v)
{
	parse_version(v, numeric_version_);
}

void
versioned_mapper::version_handler::parse_version(const std::string &version,
    numeric_version &ret)
{
	ret.clear();

	if (version.empty()) {
		throw isi_exception(get_user_text(REST_EMPTY_VERSION));
	}

	if (version.find_first_not_of("0123456789.") != std::string::npos) {
		throw isi_exception(get_user_text(REST_INVALID_VERSION_x),
		    version.c_str());
	}

	if (version.find("..") != std::string::npos) {
		throw isi_exception(get_user_text(REST_INVALID_VERSION_x),
		    version.c_str());
	}

	if (version.at(0) == '.' || version.at(version.size() - 1) == '.') {
		throw isi_exception(get_user_text(REST_INVALID_VERSION_x),
		    version.c_str());
	}

	ret.clear();
	size_t start = 0;

	/* split the version string between '.' characters and add the numbers
	 * to the numeric version vector */
	while (start < version.size()) {
		size_t end = version.find_first_of('.', start);

		if (end == std::string::npos) {
			end = version.size();
		}

		std::string sub = version.substr(start, end - start);

		int v = lexical_cast<int>(sub);
		ret.push_back(v);

		start = end + 1;
	}
}

int
versioned_mapper::version_handler::compare_versions(const version_handler &a,
    const version_handler &b)
{
	size_t min_size = std::min(a.numeric_version_.size(),
	    b.numeric_version_.size());

	for (size_t i = 0; i < min_size; ++i) {
		if (a.numeric_version_[i] > b.numeric_version_[i]) {
			return 1;
		} else if (a.numeric_version_[i] < b.numeric_version_[i]) {
			return -1;
		}
	}

	if (a.numeric_version_.size() > b.numeric_version_.size()) {
		return 1;
	} else if (a.numeric_version_.size() < b.numeric_version_.size()) {
		return -1;
	} else {
		return 0;
	}
}

bool
versioned_mapper::version_handler::can_map_to(
    const version_handler &other) const
{
	if (version_str_ == other.version_str_) {
		return true;
	}

	if (other.numeric_version_.empty() && numeric_version_.empty()) {
		return true;
	} else if (other.numeric_version_.empty() ||
	    numeric_version_.empty()) {
		return false;
	}

	if (other.numeric_version_.size() > numeric_version_.size()) {
		return false;
	} else {
		size_t last_position = other.numeric_version_.size() - 1;

		if (other.numeric_version_[last_position] >
		    numeric_version_[last_position]) {
			return false;
		}

		for (size_t i = 0; i < last_position; ++i) {
			if (other.numeric_version_[i] !=
			    numeric_version_[i]) {
				return false;
			}
		}
	}

	return true;
}

void
versioned_mapper::version_handler::parse_path(const std::string &path,
    std::string &version, std::string &suffix, bool allow_empty)
{
	suffix = "";
	version = "";

	if (path.empty()) {
		if (allow_empty)
			return;

		throw isi_exception(get_user_text(PATH_REQUIRED));
	}

	if (path[0] != '/') {
		if (allow_empty)
			return;

		throw isi_exception(
		    get_user_text(REST_MALFORMED_PATH_x), path.c_str());
	}

	// find '/' after version
	size_t sep_pos = path.find_first_of('/', 1);

	if (sep_pos == std::string::npos) {
		// There is no second '/'
		if (allow_empty) {
			version = uri_comp_decode_copy(path.substr(1));
			return;
		} else {
			throw isi_exception(
			    get_user_text(REST_MALFORMED_PATH_x), path.c_str());
		}
	} else {
		version = uri_comp_decode_copy(path.substr(1, sep_pos - 1));
	}

	if (sep_pos == path.size() - 1) {
		// '/' was the last character
		if (allow_empty)
			return;

		throw isi_exception(
		    get_user_text(REST_MALFORMED_PATH_x), path.c_str());
	}

	suffix = path.substr(sep_pos);
}

/**
 * versioned_pattern_handler
 */
versioned_mapper::versioned_pattern_handler::versioned_pattern_handler(const
    std::string &r, bool i) : uri_mapper::pattern_handler(r, i)
{ }

versioned_mapper::versioned_pattern_handler::~versioned_pattern_handler()
{ }

void
versioned_mapper::versioned_pattern_handler::add_handler_version(
    const std::string &path, uri_handler *h)
{
	std::string version;
	std::string suffix;
	versioned_mapper::version_handler::parse_path(path, version, suffix);
	versioned_mapper::version_handler v(version);

	if (suffix != rep_) {
                throw isi_exception(
			get_user_text(REST_ATTEMPTING_ADD_WRONG_PATH_x),
			path.c_str());
	}

        if (hmap_.find(path) != hmap_.end() || version_set_.count(v) != 0) {
		throw isi_exception(
		    get_user_text(REST_DUPLICATE_PATH_x), path.c_str());
        }

	version_set_.insert(v);
        hmap_[path] = h;
}

uri_handler *
versioned_mapper::versioned_pattern_handler::resolve_version(
    const version_handler &version) const
{
	std::string v_str = version.version_str_;

	for (version_set::const_iterator it = version_set_.begin(); it !=
	    version_set_.end(); ++it) {
		if (version.can_map_to(*it)) {
			v_str = it->version_str_;
		}
	}

	std::string full_path = "/" + v_str + rep_;

	if (hmap_.count(full_path) > 0) {
		return hmap_.find(full_path)->second;
	} else {
		return NULL;
	}
}

/**
 * versioned_mapper
 */
const std::string versioned_mapper::LATEST   = "latest";
const std::string versioned_mapper::UPDATED  = "updated";
const std::string versioned_mapper::VERSIONS = "versions";
const std::string versioned_mapper::CHANGED  = "changed";

versioned_mapper::versioned_mapper(
    unsigned prefix_len, const std::string &doc_root)
    : uri_mapper(prefix_len), doc_root_(doc_root)
{ }

uri_handler *
versioned_mapper::resolve(const std::string &path, uri_tokens &tokens) const
{
	std::string suffix_str;
	std::string version_str;

	try {
		version_handler::parse_path(path, version_str, suffix_str);
	} catch (isi_exception &e) {
		/* If it doesn't parse return 404 */
		return NULL;
	}

	/*
	 * If the version isn't a valid version string, assume it was omitted
	 * and default to the highest available version.
	 */
	if (version_str.find_first_not_of("0123456789.") != std::string::npos) {
		// We found a non version character
		version_set::const_reverse_iterator vit = version_set_.rbegin();

		if (vit != version_set_.rend()) {
			suffix_str = path;
			version_str = vit->version_str_;
		}
	}

	/* No handlers exist */
	if (version_set_.empty()) {
		return NULL;
	}

	version_handler version;

	try {
		version = version_handler(version_str);
	} catch (isi_exception &e) {
		/* If it doesn't parse return 404 */
		return NULL;
	}

	/* Requested version must be valid */
	if (version_set_.count(version) == 0) {
		return NULL;
	}

	versioned_pattern_handler *vph = (versioned_pattern_handler *)
	    resolve_pattern(suffix_str, tokens);

	if (vph == NULL) {
		return NULL;
	} else {
		return vph->resolve_version(version);
	}
}

void
versioned_mapper::add(const std::string &path, uri_handler *h, bool internal)
{
	std::string suffix_str;
	std::string version_str;
	versioned_mapper::version_handler::parse_path(path, version_str,
	    suffix_str);
	versioned_mapper::version_handler version(version_str);

	versioned_pattern_handler *vph = (versioned_pattern_handler *)
	    directory_[suffix_str];

	/* create a new versioned_pattern_handler if one doesn't exist */
	if (vph == NULL) {
		vph = new versioned_pattern_handler(suffix_str, internal);
		std::auto_ptr<versioned_pattern_handler> p(vph);

		std::string prefix = vph->parse(suffix_str, prefix_len_);

		/* find any duplicates */
		std::pair<pattern_map::const_iterator,
		    pattern_map::const_iterator> r;
		r = patterns_.equal_range(prefix);

		for (pattern_map::const_iterator it = r.first; it != r.second;
		    ++it) {
			if (vph->conflicts_with(*it->second)) {
				throw isi_exception(
				    get_user_text(REST_DUPLICATE_PATH_x),
					path.c_str());
			}
		}

		/* otherwise insert */
		pattern_map::iterator it;
		p.release();
		it = patterns_.insert(std::make_pair(prefix, vph));
		directory_[suffix_str] = vph;
	}

	vph->add_handler_version(path, h);
	version_set_.insert(version);
}

void
versioned_mapper::get_uri_list(const std::string &prefix, bool internal,
    std::list<std::string> &ret) const
{
	ret.clear();

	std::string suffix_str;
	std::string version_str;

	try {
		version_handler::parse_path(prefix, version_str, suffix_str,
		    true);
	} catch (isi_exception &e) {
		/* If it doesn't parse return an empty list */
		return;
	}

	directory_map::const_iterator lb;
	directory_map::const_iterator ub;

	/* No handlers exist */
	if (version_set_.empty()) {
		return;
	}

	version_handler v;

	if (!version_str.empty()) {
		try {
			v = version_handler(version_str);
		} catch (isi_exception &e) {
			return;
		}

		/* Requested version must be valid */
		if (version_set_.count(v) == 0) {
			return;
		}
	}

	if (suffix_str.empty())  {
		lb = directory_.begin();
		ub = directory_.end();
	} else {
		std::string lb_uri = suffix_str + '/';
		std::string ub_uri = suffix_str + char('/' + 1);

		lb = directory_.lower_bound(lb_uri);
		ub = directory_.upper_bound(ub_uri);
	}

	/*
	* Filter out internal unless that argument is given.
	*/
	for (directory_map::const_iterator it = lb; it != ub; ++it) {
		if (it->second != NULL) {
			if (internal || !it->second->is_internal()) {
				if (version_str.empty()) {
					/* Add all versions */
					pattern_handler::const_iterator hit;
					for (hit = it->second->begin(); hit !=
					    it->second->end(); ++hit) {
						if (hit->second != NULL) {
							ret.push_back(
							    hit->first);
						}
					}
				} else {
					versioned_pattern_handler *vph =
					    (versioned_pattern_handler *)
					    it->second;

					if (vph->resolve_version(v) != NULL) {
						ret.push_back("/" +
						    version_str + it->first);
					}
				}
			}
		}
	}
}

bool
versioned_mapper::handle_metadata_requests(request &input,
    response &output) const
{
	if (input.get_method() != request::GET) {
		return false;
	}

	std::string latest_prefix = "/" + LATEST;
	std::string updated_prefix = "/" + UPDATED;
	std::string versions_prefix = "/" + VERSIONS;
	std::string changed_prefix = "/" + CHANGED;

	if (input.get_uri() == "" || input.get_uri() == "/") {
		handle_landing_page(input, output);
		return true;
	}

	if (input.get_uri().find(latest_prefix) == 0) {
		handle_latest(input, output);
		return true;
	}

	if (input.get_uri().find(updated_prefix) == 0) {
		handle_updated(input, output);
		return true;
	}

	if (input.get_uri().find(versions_prefix) == 0) {
		handle_versions(input, output);
		return true;
	}

	if (input.get_uri().find(changed_prefix) == 0) {
		handle_changed(input, output);
		return true;
	}

	return false;
}

void
versioned_mapper::handle_landing_page(request &input, response &output) const
{
	// Read overview document
	std::string overview_txt = "";
	std::string root = get_doc_root();
	std::string overview_doc_path = root + '/' + handler_doc::OVERVIEW_DOC;
	std::ifstream overview_doc(overview_doc_path.c_str());

	if (overview_doc.is_open()) {
		std::stringstream buffer;
		buffer << overview_doc.rdbuf();
		overview_doc.close();
		overview_txt = buffer.str();
		ilog(IL_TRACE, "Loaded file: %s", overview_doc_path.c_str());

		// Assemble list of all URIs.
		std::list<std::string> uris;
		get_uri_list(input.get_uri(), false, uris);
		std::string uris_str = "";
		bool is_first = true;
		for (std::list<std::string>::iterator it = uris.begin(); it !=
		    uris.end(); ++it) {
			if (!is_first) {
				uris_str.append("\n");
			}
			is_first = false;
			uris_str.append(*it);
		}

		// Parse URI list into the overview text file at <<directory>>.
		overview_txt = boost::replace_all_copy(
		    overview_txt, "<<directory>>", uris_str);

	} else {
		overview_txt = "Overview doc ";
		overview_txt.append(overview_doc_path.c_str());
		overview_txt.append(" does not exist!");
		ilog(IL_DEBUG, overview_txt.c_str());
	}

	output.set_content_type(api_content::TEXT_PLAIN);
	output.get_body() = overview_txt;
}

void
versioned_mapper::handle_updated(request &input,
    response &output) const
{
	Json::Value &j_output = output.get_json();
	j_output["updated"] = Json::arrayValue;
	Json::Value &j_updated = j_output["updated"];
	j_output["removed"] = Json::arrayValue;
	Json::Value &j_removed = j_output["removed"];

	std::string prefix_str;
	std::string suffix_str;
	versioned_mapper::version_handler::parse_path(input.get_uri(),
	    prefix_str, suffix_str);

	versioned_pattern_handler *vph = NULL;

	if (!version_set_.empty()) {
		vph = (versioned_pattern_handler *) resolve_pattern(suffix_str,
		    input.tokens());
	}

	if (vph == NULL) {
		throw api_exception(AEC_NOT_FOUND,
                    get_user_text(REST_PATH_NOT_FOUND_x),
                    input.get_uri().c_str());
	}

	for (pattern_handler::const_iterator hit = vph->begin(); hit !=
	    vph->end(); ++hit) {
		if (hit->second == NULL) {
			j_removed.append(hit->first);
		} else {
			j_updated.append(hit->first);
		}
	}
}

void
versioned_mapper::handle_latest(request &input,
    response &output) const
{
	Json::Value &j_output = output.get_json();

	std::string prefix_str;
	std::string suffix_str;
	versioned_mapper::version_handler::parse_path(input.get_uri(),
	    prefix_str, suffix_str, true);

	if (!suffix_str.empty() && suffix_str != "/") {
		throw api_exception(AEC_NOT_FOUND,
                    get_user_text(REST_PATH_NOT_FOUND_x),
                    input.get_uri().c_str());
	}

	version_set::const_reverse_iterator vit = version_set_.rbegin();

	if (vit == version_set_.rend()) {
		return;
	}

	j_output["latest"] = vit->version_str_;
}

void
versioned_mapper::handle_versions(request &input,
    response &output) const
{
	Json::Value &j_output = output.get_json();
	j_output["versions"] = Json::arrayValue;
	Json::Value &j_versions = j_output["versions"];

	std::string prefix_str;
	std::string suffix_str;
	versioned_mapper::version_handler::parse_path(input.get_uri(),
	    prefix_str, suffix_str, true);

	// If there is no path, return the global version list
	if (suffix_str.empty() || suffix_str == "/") {
		for (version_set::const_iterator vit = version_set_.begin();
		    vit != version_set_.end(); ++vit) {
			j_versions.append(vit->version_str_);
		}
		return;
	}

	versioned_pattern_handler *vph = NULL;

	if (!version_set_.empty()) {
		vph = (versioned_pattern_handler *) resolve_pattern(suffix_str,
		    input.tokens());
	}

	if (vph == NULL) {
		throw api_exception(AEC_NOT_FOUND,
                    get_user_text(REST_PATH_NOT_FOUND_x),
                    input.get_uri().c_str());
	}

	for (version_set::const_iterator vit = version_set_.begin();
	    vit != version_set_.end(); ++vit) {
		uri_handler *h = vph->resolve_version(*vit);

		if (h != NULL) {
			std::string uri = "/";
			uri.append(vit->version_str_);
			uri.append(suffix_str);
			j_versions.append(uri);
		}
	}
}

void
versioned_mapper::handle_changed(request &input,
    response &output) const
{
	Json::Value &j_output = output.get_json();
	j_output["changed"] = Json::arrayValue;
	Json::Value &j_changed = j_output["changed"];
	j_output["removed"] = Json::arrayValue;
	Json::Value &j_removed = j_output["removed"];

	std::string changed_str;
	std::string suffix_str;
	versioned_mapper::version_handler::parse_path(input.get_uri(),
	    changed_str, suffix_str, true);

	if (changed_str.empty() || suffix_str.empty()) {
		return;
	}

	std::string version_str;
	std::string path_str;
	versioned_mapper::version_handler::parse_path(suffix_str,
	    version_str, path_str, true);

	if (version_str.empty()) {
		throw api_exception(AEC_NOT_FOUND,
                    get_user_text(REST_PATH_NOT_FOUND_x),
                    input.get_uri().c_str());
	}

	version_handler v;

	try {
		v= version_handler(version_str);
	} catch (isi_exception &e) {
		throw api_exception(AEC_NOT_FOUND,
                    get_user_text(REST_PATH_NOT_FOUND_x),
                    input.get_uri().c_str());
	}

	/* Requested version must be valid */
	if (version_set_.count(v) == 0) {
		throw api_exception(AEC_NOT_FOUND,
                    get_user_text(REST_PATH_NOT_FOUND_x),
                    input.get_uri().c_str());
	}

	directory_map::const_iterator lb;
	directory_map::const_iterator ub;

	if (path_str.empty())  {
		lb = directory_.begin();
		ub = directory_.end();
	} else {
		std::string lb_uri = path_str + '/';
		std::string ub_uri = path_str + char('/' + 1);

		lb = directory_.lower_bound(lb_uri);
		ub = directory_.upper_bound(ub_uri);
	}

	/*
	* Filter out internal unless that argument is given.
	*/
	for (directory_map::const_iterator it = lb; it != ub; ++it) {
		if (it->second != NULL) {
			versioned_pattern_handler *vph =
			    (versioned_pattern_handler *) it->second;

			std::string full_path = "/" + version_str + it->first;

			if (vph->contains(full_path)) {
				uri_handler *h = vph->get(full_path);

				if (h == NULL) {
					j_removed.append(full_path);
				} else {
					j_changed.append(full_path);
				}
			}
		}
	}
}

void
versioned_mapper::log() const
{
	std::stringstream ss;
	ss << "Version list: ";
	for (version_set::const_iterator it = version_set_.begin(); it !=
	    version_set_.end(); ++it) {
		ss << it->version_str_;
		ss << " ";
	}

	ilog(IL_DEBUG, "%s", ss.str().c_str());
}

