#ifndef __URI_MAPPER_H__
#define __URI_MAPPER_H__

#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <isi_rest_server/method_stats.h>
#include <isi_rest_server/uri_tokens.h>

class uri_handler;
class request;
class response;

class uri_mapper {
public:
	struct pattern {
		enum type {
			LITERAL,
			SINGLE,
			ZERO_OR_MORE,
			ONE_OR_MORE,
		};

		type        type_;
		std::string name_;
	};

	typedef std::vector<pattern> pattern_vec;
	typedef std::map<std::string, uri_handler *> uri_handler_map;

	class pattern_handler {
	public:
		pattern_handler(const std::string &r, bool i);
		pattern_handler(const std::string &r, uri_handler *h, bool i);
		virtual ~pattern_handler();

		virtual std::string parse(const std::string &path,
		    unsigned pfx_len);

		virtual bool conflicts_with(const pattern_handler &x) const;

		virtual bool matches(const std::string &path,
		    std::string::size_type p, uri_tokens &toks) const;

		virtual void log_method_stats(bool clear) const;

		typedef uri_handler_map::iterator iterator;
		typedef uri_handler_map::const_iterator const_iterator;

		inline bool contains(const std::string &path) const;
		inline uri_handler *get(const std::string &path) const;
		inline iterator begin();
		inline iterator end();
		inline const_iterator begin() const;
		inline const_iterator end() const;
		inline bool is_internal() const;

	protected:
		std::string  rep_;
		pattern_vec  pvec_;
		uri_handler_map hmap_;
		bool         internal_;
	};

	typedef std::multimap<std::string, pattern_handler *>  pattern_map;
	typedef std::map<std::string, const pattern_handler *> directory_map;
	typedef pattern_map::iterator                          iterator;
	typedef pattern_map::const_iterator                    const_iterator;

	/** construct uri mapper.
	 *
	 * @param pl Minimum component prefix length of any path in the
	 *           mapper that is used to speed up matches.
	 */
	explicit uri_mapper(unsigned prefix_len);

	virtual ~uri_mapper();

	/** Find pattern for path.
	 * @param path
	 * @param toks positional uri tokens - will be cleared first
	 *
	 * @return pattern_handler or NULL if not found.
	 */
	virtual pattern_handler *resolve_pattern(const std::string &path,
	    uri_tokens &toks) const;

	/** Find handler for path.
	 * @param path
	 * @param toks positional uri tokens - will be cleared first
	 *
	 * @return handler or NULL if not found.
	 */
	virtual uri_handler *resolve(const std::string &path,
	    uri_tokens &toks) const;

	/** Find handler for path.
	 * @param input  request
	 * @param output response
	 * @return handler or NULL if not found.
	 */
	virtual uri_handler *resolve(request &input, response &output) const;

	/** Add a handler.
	 *
	 * Handler ownership is passed to this.  NULL can be used for the
	 * handler to effectively disable a path. Path specification must be
	 * unique.
	 *
	 * @param u uri pattern
	 * @param h handler
	 * @param i internal isilon handler, affects discovery
	 */
	virtual void add(const std::string &u, uri_handler *h = NULL,
	    bool i = false);

	/** Add all handlers from another map to this map.
	 *
	 * This will add all the handlers from another map to this map.  This
	 * allows isi_papi_d to combine maps declared in different libraries.
	 * The handlers will be removed from the other mapper.
	 *
	 * @ param other the other uri_mapper
	 */
	virtual void merge_all(uri_mapper &other);

	void get_directory(const request &input, response &output,
	    bool internal) const;

	/**
	 * This function handles metadata requests that don't get executed by a
	 * handler.
	 *
	 * @param input  request
	 * @param output response
	 * @return whether a metadata operation was performed
	 */
	virtual bool handle_metadata_requests(request &input,
	    response &output) const;

	/** Return number of path patterns in this */
	inline unsigned size() const;

	/** @{ iteration function */

	inline iterator begin();

	inline iterator end();

	inline const_iterator begin() const;

	inline const_iterator end() const;

	/** @} */

	/** Get full map of pattern strings for directory */
	inline const directory_map &directory() const;

	/** Call stats for unknown resources */
	mutable method_stats unknown_stats_;

	/** Call stats for describe calls */
	mutable method_stats describe_stats_;

	/** Log above stats and per-handler stats optionally clearing */
	virtual void log_method_stats(bool clear) const;

	/** Get a list of uris that begin with prefix */
	virtual void get_uri_list(const std::string &prefix, bool internal,
	    std::list<std::string> &ret) const;

protected:
	pattern_map   patterns_;
	directory_map directory_;
	unsigned      prefix_len_;
};

/**
 * A mapper that enforces versioning in the first token of the URI.
 */
class versioned_mapper : public uri_mapper {
public:
	static const std::string LATEST;
	static const std::string UPDATED;
	static const std::string VERSIONS;
	static const std::string CHANGED;

        typedef std::vector<int> numeric_version;

	struct version_handler {
		std::string version_str_;
		numeric_version numeric_version_;

		explicit version_handler(const std::string &v = "0");

		static void parse_version(const std::string &version,
		    numeric_version &ret);

		static int compare_versions(const version_handler &a, const
		    version_handler &b);

		inline bool operator<(const version_handler &other) const;

		bool can_map_to(const version_handler &other) const;

		static void parse_path(const std::string &path, std::string
		    &version, std::string &suffix, bool allow_empty = false);
	};

	typedef std::set<version_handler> version_set;

	class versioned_pattern_handler : public uri_mapper::pattern_handler {
	public:
		version_set version_set_;

		versioned_pattern_handler(const std::string &r, bool i);
		virtual ~versioned_pattern_handler();

		void add_handler_version(const std::string &path,
		    uri_handler *h);

		uri_handler *resolve_version(const version_handler &version)
		    const;
	};

	/** construct uri mapper.
	 *
	 * @param pl Minimum component prefix length of any path in the
	 *           mapper that is used to speed up matches.
	 */
	explicit versioned_mapper(
	    unsigned prefix_len, const std::string &doc_root);

	virtual uri_handler *resolve(const std::string &path,
	    uri_tokens &toks) const;

	virtual void add(const std::string &u, uri_handler *h = NULL,
	    bool i = false);

	/** Get a list of uris that begin with prefix */
	virtual void get_uri_list(const std::string &prefix, bool internal,
	    std::list<std::string> &ret) const;

	/*
	 * This function implements metadata reqests such as: /latest,
	 * /updated, /versions, and /changed
	 */
	virtual bool handle_metadata_requests(request &input,
	    response &output) const;
	
	virtual void handle_landing_page(request &input,
	    response &output) const;

	/*
	 * Returns the highest version number in the API.
	 */
	virtual void handle_latest(request &input, response &output) const;

	/*
	 * The uri /updated/<URI> will return the version numbers when <URI>
	 * was changed.
	 */
	virtual void handle_updated(request &input, response &output) const;

	/*
	 * The uri /versions returns a list of valid versions in the API.  The
	 * uri /versions/<URI> returns a list of valid versions for that uri.
	 */
	virtual void handle_versions(request &input, response &output) const;

	/*
	 * The uri /changed/<version>/<prefix> will return a list of the uris
	 * that were changed in the given version.  The prefix will limit the
	 * returned list and is optional.
	 */
	virtual void handle_changed(request &input, response &output) const;

	inline std::string get_doc_root() const;
	inline void set_doc_root(const std::string &doc_root);

	void log() const;

protected:
	version_set version_set_;

private:
	std::string doc_root_;
};

/*  _       _ _
 * (_)_ __ | (_)_ __   ___
 * | | '_ \| | | '_ \ / _ \
 * | | | | | | | | | |  __/
 * |_|_| |_|_|_|_| |_|\___|
 */

bool
versioned_mapper::version_handler::operator<(
    const version_handler &other) const
{
	return compare_versions(*this, other) < 0;
}

unsigned
uri_mapper::size() const
{
	return patterns_.size();
}

uri_mapper::iterator
uri_mapper::begin()
{
	return patterns_.begin();
}

uri_mapper::iterator
uri_mapper::end()
{
	return patterns_.end();
}

uri_mapper::const_iterator
uri_mapper::begin() const
{
	return patterns_.begin();
}

uri_mapper::const_iterator
uri_mapper::end() const
{
	return patterns_.end();
}

const uri_mapper::directory_map &
uri_mapper::directory() const
{
	return directory_;
}

uri_mapper::pattern_handler::iterator
uri_mapper::pattern_handler::begin()
{
	return hmap_.begin();
}

uri_mapper::pattern_handler::iterator
uri_mapper::pattern_handler::end()
{
	return hmap_.end();
}

uri_mapper::pattern_handler::const_iterator
uri_mapper::pattern_handler::begin() const
{
	return hmap_.begin();
}

uri_mapper::pattern_handler::const_iterator
uri_mapper::pattern_handler::end() const
{
	return hmap_.end();
}

bool
uri_mapper::pattern_handler::is_internal() const
{
	return internal_;
}

bool
uri_mapper::pattern_handler::contains(const std::string &path) const
{
	if (hmap_.count(path) != 0) {
		return true;
	}

	return false;
}

uri_handler *
uri_mapper::pattern_handler::get(const std::string &path) const
{
	if (hmap_.count(path) != 0) {
		return hmap_.at(path);
	}

	return NULL;
}

std::string versioned_mapper::get_doc_root() const
{
	return doc_root_;
}


void versioned_mapper::set_doc_root(const std::string &doc_root)
{
	doc_root_ = doc_root;
}

#endif

