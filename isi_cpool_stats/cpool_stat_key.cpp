#include "cpool_stat_key.h"

int
cpool_stat_account_key::compare(const cpool_stat_account_key &other) const
{
	if (other.key_ > key_)
		return -1;

	if (other.key_ < key_)
		return 1;

	return 0;
}

int
cpool_stat_account_query_result_key::compare(
    const cpool_stat_account_query_result_key &other) const
{
	if (other.key_ > key_)
		return -1;

	if (other.key_ < key_)
		return 1;

	if (other.interval_ > interval_)
		return -1;

	if (other.interval_ < interval_)
		return 1;

	return 0;
}
