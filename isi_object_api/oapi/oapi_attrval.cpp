#include "oapi_attrval.h"

#include "isi_ilog/ilog.h"

#include <isi_object/ostore.h>


void 
oapi_attrval::set_value(void * val, int len)
{

	reset();
	value_ = val;
	len_ = len;	
}

void 
oapi_attrval::reset()
{
	if (value_) {
		iobj_object_attribute_release(value_);
		value_ = 0;
	}
	
	len_ = 0;
}

oapi_attrval::~oapi_attrval()
{

	reset();
}

