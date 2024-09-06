#include "cpool_stat.h"

void cpool_stat::copy(const cpool_stat &other)
{
	bytes_in = other.bytes_in;
	bytes_out = other.bytes_out;
	num_gets = other.num_gets;
	num_puts = other.num_puts;
	num_deletes = other.num_deletes;
	total_usage = other.total_usage;
}

cpool_stat *
cpool_stat::clone() const
{
	cpool_stat *ret_val = new cpool_stat();
	ret_val->copy(*this);

	return ret_val;
}
