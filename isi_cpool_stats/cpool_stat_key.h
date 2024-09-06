#ifndef __ISI_CPOOL_STAT_KEY_H__
#define __ISI_CPOOL_STAT_KEY_H__

#include <isi_util/isi_assert.h>
#include <isi_util/util.h>
#include <isi_cpool_config/cpool_config.h>

/**
 * A key class for referencing stats by a unique account key
 */
class cpool_stat_account_key {

public:
	cpool_stat_account_key() : key_(0) {};
	cpool_stat_account_key(account_key_t acct_key) : key_(acct_key) {};
	cpool_stat_account_key(const cpool_stat_account_key& that)
	{
		copy(that);
	};

	cpool_stat_account_key& operator=(const cpool_stat_account_key &that)
	{
		copy(that);
		return(*this);
	};

	int compare(const cpool_stat_account_key &other) const;

	inline cpool_stat_account_key *clone() const
	{
		return new cpool_stat_account_key(*this);
	};

	inline account_key_t get_account_key() const
	{
		return key_;
	};

protected:
	void copy(const cpool_stat_account_key& that)
	{
		key_ = that.key_;
	};

private:
	account_key_t key_;
};


/**
 * A key class for referencing stats by account and time span
 */
class cpool_stat_account_query_result_key {

public:
	cpool_stat_account_query_result_key(account_key_t acct_key,
	    unsigned int time_delta) : key_(acct_key), interval_(time_delta) {};
	cpool_stat_account_query_result_key() : key_(0), interval_(0) {};
	cpool_stat_account_query_result_key(
	    const cpool_stat_account_query_result_key &that)
	{
		copy(that);
	};

	int compare(const cpool_stat_account_query_result_key &other) const;

	cpool_stat_account_query_result_key *clone() const
	{
		return new cpool_stat_account_query_result_key(*this);
	};

	inline account_key_t get_account_key() const
	{
		return key_;
	};

	inline unsigned int get_interval() const
	{
		return interval_;
	};

	cpool_stat_account_query_result_key& operator =
		(const cpool_stat_account_query_result_key &that)
	{
		copy(that);
		return (*this);
	};

protected:
	void copy(const cpool_stat_account_query_result_key &that)
	{
		key_ = that.key_;
		interval_ = that.interval_;
	};

private:
	account_key_t key_;
	unsigned int interval_;
};


#endif // __ISI_CPOOL_STAT_KEY_H__
