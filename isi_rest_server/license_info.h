#ifndef __LICENSE_INFO_H__
#define __LICENSE_INFO_H__

#include <pthread.h>

#include <isi_licensing/licensing.h>

/** Licensing information cache and thread protection */
class license_info {
public:
	/* JSON Display Fields */
	enum field {
		NONE,
		ID,
		NAME,
		STATUS,
		EXPIRATION,
		DURATION
	};

	/** Return singleton */
	static inline license_info &instance();

	/** Default time we'll use cached result before auto updating */
	static const time_t DEFAULT_CACHE_TIME = 5 * 60;

	~license_info();

	/** Return license status for application @a name */
	isi_licensing_status status(const char *name);

	/** Return license module for application @a name */
	struct isi_licensing_module find(const char *name);

	/**
	 * return an array of license names as a char **
	 * Does not need to be freed as it is allocated statically
	 */
	const char** get();

	/** Check valid license for application @a name.
	 *
	 * @throw if license is invalid in any way.
	 */
	void check(const char *name);

	void check(const char *name, const char *field, const char *reason);

	/** Return current cache time */
	inline time_t cache_time() const;

	/** Set new cache time, return old cache time.
	 *
	 * @a ct < 0 disables lazy refresh, call explicitly.
	 */
	time_t cache_time(time_t ct);

private:
	static license_info s_instance;

	license_info();

	time_t get_time();

	pthread_mutex_t	      mtx_;
	time_t                last_update_;
	time_t                cache_time_;

	/* uncopyable, unassignable */
	license_info(const license_info &);
	license_info &operator=(const license_info &);
};


/*  _       _ _
 * (_)_ __ | (_)_ __   ___
 * | | '_ \| | | '_ \ / _ \
 * | | | | | | | | | |  __/
 * |_|_| |_|_|_|_| |_|\___|
 */

license_info &
license_info::instance()
{
	return s_instance;
}

time_t
license_info::cache_time() const
{
	return cache_time_;
}

#endif
