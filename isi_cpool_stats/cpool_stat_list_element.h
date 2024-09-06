#ifndef __CPOOL_STAT_LIST_ELEMENT_H__
#define __CPOOL_STAT_LIST_ELEMENT_H__

/**
 * A template container for pairing keys to values (ostensibly to be used in a
 * map implementation).
 *
 * NOTE * This class assumes shallow values and keys; that is,
 * shallow copies of keys and values are made at assignment time, so embedded
 * pointers will not function correctly.
 */
////保存<account,val>.account是一个int,value是一个cpool_stat
template <class key_class, class value_class>
class cpool_stat_list_element
{
public:
	/**
	 * Constructor - sets initial value of empty_
	 */
	cpool_stat_list_element() : empty_(true) {};

	/**
	 * Constructor + initial values
	 */
	cpool_stat_list_element(const key_class &key, const value_class &value)
	{
		set(key, value);
	};

	/**
	 * Returns boolean indicating whether this element is populated
	 */
	inline bool is_empty() const
	{
		return empty_;
	};

	/**
	 * Resets the value of this element by marking it empty
	 */
	inline void clear()
	{
		empty_ = true;
	};

	/**
	 * Returns a pointer to the key.  If this element is empty (e.g., there
	 * is no key), a NULL pointer is returned
	 */
	inline const key_class *get_key() const
	{
		if (is_empty())
			return NULL;

		return &key_;
	};

	/**
	 * Returns a pointer to the value.  If this element is empty (e.g.,
	 * there is no value), a NULL pointer is returned
	 */
	inline value_class *get_value()
	{
		if (is_empty())
			return NULL;

		return &value_;
	};

	/**
	 * Returns a read-only pointer to the value.  If this element is empty
	 * (e.g., there is no value), a NULL pointer is returned
	 */
	inline const value_class *get_read_only_value() const
	{
		if (is_empty())
			return NULL;

		return &value_;
	};

	/**
	 * Sets the key and value for this element by doing a shallow copy
	 * (e.g., memcpy).  After key and value are set, element is marked as
	 * non-empty.
	 */
	void set(const key_class &new_key, const value_class &new_value)
	{
		memcpy(&key_, &new_key, sizeof(new_key));
		memcpy(&value_, &new_value, sizeof(new_value));
		empty_ = false;
	};

private:
	bool empty_;
	key_class key_;
	value_class value_;
};

#endif // __CPOOL_STAT_LIST_ELEMENT_H__
