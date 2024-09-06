#ifndef __ISI_CPOOL_STAT_H__
#define __ISI_CPOOL_STAT_H__

#include <isi_util/isi_assert.h>
#include <isi_util/util.h>

#define CP_STAT_BYTES_IN	(1 << 0)
#define CP_STAT_BYTES_OUT	(1 << 1)
#define CP_STAT_NUM_GETS	(1 << 2)
#define CP_STAT_NUM_PUTS	(1 << 3)
#define CP_STAT_NUM_DELETES	(1 << 4)
#define CP_STAT_TOTAL_USAGE	(1 << 5)

class cpool_stat {

public:
	cpool_stat() : bytes_in(0), bytes_out(0), num_gets(0),
		num_puts(0), num_deletes(0), total_usage(0) {};
	cpool_stat(uint64_t b_in, uint64_t b_out,
	    uint64_t n_gets, uint64_t n_puts, uint64_t n_deletes,
	    uint64_t t_usage) :
	    bytes_in(b_in), bytes_out(b_out),
	    num_gets(n_gets), num_puts(n_puts),
	    num_deletes(n_deletes), total_usage(t_usage) {};

	inline void clear() {
		bytes_in = 0;
		bytes_out = 0;
		num_gets = 0;
		num_puts = 0;
		num_deletes = 0;
		total_usage = 0;
	}

	inline void set_bytes_in(uint64_t val) {
		bytes_in = val;
	};
	inline uint64_t get_bytes_in() const {
		return bytes_in;
	};

	inline void set_bytes_out(uint64_t val) {
		bytes_out = val;
	};
	inline uint64_t get_bytes_out() const {
		return bytes_out;
	};

	inline void set_num_gets(uint64_t val) {
		num_gets = val;
	};
	inline uint64_t get_num_gets() const {
		return num_gets;
	};

	inline void set_num_puts(uint64_t val) {
		num_puts = val;
	};
	inline uint64_t get_num_puts() const {
		return num_puts;
	};
	inline void set_num_deletes(uint64_t val) {
		num_deletes = val;
	};
	inline uint64_t get_num_deletes() const {
		return num_deletes;
	};
	inline void set_total_usage(uint64_t val) {
		total_usage = val;
	};
	inline uint64_t get_total_usage() const {
		return total_usage;
	};
	inline void add_stats(uint64_t in_bytes_in, uint64_t in_bytes_out,
	    uint64_t in_num_gets, uint64_t in_num_puts,
	    uint64_t in_num_deletes, uint64_t in_total_usage) {
		bytes_in += in_bytes_in;
		bytes_out += in_bytes_out;
		num_gets += in_num_gets;
		num_puts += in_num_puts;
		num_deletes += in_num_deletes;
		total_usage += in_total_usage;
	};
	inline bool is_zero() const
	{
		return
		    bytes_in == 0 &&
		    bytes_out == 0 &&
		    num_gets == 0 &&
		    num_puts == 0 &&
		    num_deletes == 0 &&
		    total_usage == 0;
	};

	inline void add(const cpool_stat &that)
	{
		add_stats(that.bytes_in, that.bytes_out,
		    that.num_gets, that.num_puts, that.num_deletes,
		    that.total_usage);
	}

	void copy(const cpool_stat &other);

	cpool_stat *clone() const;
protected:

	uint64_t bytes_in;
	uint64_t bytes_out;
	uint64_t num_gets;
	uint64_t num_puts;
	uint64_t num_deletes;
	uint64_t total_usage;
};
#endif // __ISI_CPOOL_STAT_H__
