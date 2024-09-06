#ifndef __SCHEDULER_PRIVATE_H__
#define __SCHEDULER_PRIVATE_H__

#ifndef SCHED_PRIVATE
#error "Don't include scheduler_private.h unless you know what "\
    "you're doing, include scheduler.h instead."
#endif
#undef SCHED_PRIVATE

#include <set>

class schedule_item
{
protected:
	int				min_;
	int				max_;
	std::set<int>			valid_values_;

	/* Default/copy construction and assignment are prohibited. */
	schedule_item();
	schedule_item(const schedule_item &);
	schedule_item &operator=(const schedule_item &);

public:
	schedule_item(int min, int max);
	virtual ~schedule_item();

	virtual void parse(const char *token_string,
	    struct isi_error **error_out);
	void advance(int &start, bool &advanced, bool &overflowed);
	bool is_valid(int value) const;
	int reset(void) const;
};

class schedule_item__second : public schedule_item
{
protected:
	/* Copy construction and assignment are prohibited. */
	schedule_item__second(const schedule_item__second &);
	schedule_item__second &operator=(const schedule_item__second &);

public:
	/* range is [0, 60] due to leap seconds */
	schedule_item__second() : schedule_item(0, 60) { }
	~schedule_item__second() { }
};

class schedule_item__minute : public schedule_item
{
protected:
	/* Copy construction and assignment are prohibited. */
	schedule_item__minute(const schedule_item__minute &);
	schedule_item__minute &operator=(const schedule_item__minute &);

public:
	schedule_item__minute() : schedule_item(0, 59) { }
	~schedule_item__minute() { }
};

class schedule_item__hour : public schedule_item
{
protected:
	/* Copy construction and assignment are prohibited. */
	schedule_item__hour(const schedule_item__hour &);
	schedule_item__hour &operator=(const schedule_item__hour &);

public:
	schedule_item__hour() : schedule_item(0, 23) { }
	~schedule_item__hour() { }
};

class schedule_item__day_of_month : public schedule_item
{
protected:
	/* Copy construction and assignment are prohibited. */
	schedule_item__day_of_month(const schedule_item__day_of_month &);
	schedule_item__day_of_month &operator=(
	    const schedule_item__day_of_month &);

public:
	schedule_item__day_of_month() : schedule_item(1, 31) { }
	~schedule_item__day_of_month() { }
};

class schedule_item__month : public schedule_item
{
protected:
	/* Copy construction and assignment are prohibited. */
	schedule_item__month(const schedule_item__month &);
	schedule_item__month &operator=(const schedule_item__month &);

public:
	schedule_item__month() : schedule_item(0, 11) { }
	~schedule_item__month() { }
};

class schedule_item__day_of_week : public schedule_item
{
protected:
	/* Copy construction and assignment are prohibited. */
	schedule_item__day_of_week(const schedule_item__day_of_week &);
	schedule_item__day_of_week &operator=(
	    const schedule_item__day_of_week &);

public:
	schedule_item__day_of_week() : schedule_item(0, 6) { }
	~schedule_item__day_of_week() { }
};

#endif // __SCHEDULER_PRIVATE_H__
