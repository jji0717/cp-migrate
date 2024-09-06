#ifndef __OAPI_ATTRVAL_H__
#define __OAPI_ATTRVAL_H__


/**
 * Thin wrapper of the object library attribute value.
 * This is capable of garbage collecting the value.
 * MT-Unsafe.
 */
class oapi_attrval {

public:	
	oapi_attrval() : len_(0), value_(0) {}
	
	~oapi_attrval();

	/**
	 * Return the value length
	 */
	int length() const { return len_; }
	
	/**
	 * Return the value buffer
	 * The value returned should be intepreted as read-only.
	 */
	const void * value() const { return value_; }
	
	void set_value(void * val, int len);	
private:
	
	int len_;
	void * value_;
	
	void reset();
	// not implemented:
	oapi_attrval(const oapi_attrval & other);
	oapi_attrval & operator= (const oapi_attrval & other);

};

#endif //__OAPI_ATTRVAL_H__

