#ifndef __MULTIPART_PROCESSOR_H__
#define __MULTIPART_PROCESSOR_H__

#include <map>
#include <string>

#include "api_headers.h"
#include "request.h"
#include "response.h"


/**
 * Interface defining the part handler
 */
class ipart_handler {
public:
	
	const api_header_map &header_map() const
	{
		return header_map_;
	}
	
	/**
	 * Get the header defined per-part
	 * @return true if the header is found, false otherwise
	 */
	virtual bool get_header(const std::string &header, std::string &value)
	    const;
	
	/**
	 * Set the per-part header
	 */	
	void set_header(const std::string &header, const std::string &value);
	
	/**
	 * Notified by the processor when headers are captured.
	 */
	virtual bool on_headers_ready() = 0;
	 
	/**
	 * Notified by the processor of the data.
	 * @param buff - the data buffer
	 * @return true if the handler wishes to continue to receive data
	 * false otherwise  
	 */
	virtual bool on_new_data(const char *buff, size_t len) = 0;
	
	/**
	 * Notified by the process of the end of data
	 * This will only be called when data in this part has been
	 * successfully processed and all previous calls too n_headers_ready
	 * and on_new_data returned true 
	 */
	virtual void on_data_end() = 0;
	
	/**
	 * clear the handler 
	 */	
	virtual void clear(); 

protected:

	ipart_handler() {}
	virtual ~ipart_handler() {}

protected:
	api_header_map header_map_;

private:
	// disable copy:
	ipart_handler(const ipart_handler &other);
	ipart_handler &operator = (const ipart_handler &other);
	
};

/**
 * Interface which is the recipient of the data 
 * parsed by the processor.
 * The implementation is responsible for creating the concrete
 * part handler. 
 */
class imultipart_handler {
public:	
		
	/**
	 * Notified by the processor of a newly found part
	 * @return the multipart_part, if it is NULL, the processor
	 *  will stop. The part_handler should be instantiated by 
	 *  the multipart_handler.
	 */
	virtual ipart_handler *on_new_part() = 0;

	/**
	 * Notified by the processor of end of an part
	 * @return true if to continue, false otherwise
	 */
	virtual bool on_part_end(ipart_handler *part_handler) = 0;
	
	/**
	 * Notified by the processor on the finishing of all parts
	 * or on end. The handler can destruct itself only when
	 * this is received.
	 */
	virtual void on_end() = 0;

protected:
	imultipart_handler() {}
	virtual ~imultipart_handler() {}
private:
	// disable copy:
	imultipart_handler(const imultipart_handler &other);
	imultipart_handler &operator = (const imultipart_handler &other);

};

/**
 * Class encapsulating the processing of mulitpart/form-data handling
 * used in post or put files. This class is also capable of handling
 * non-multipart data -- through the same interface.
 */
class multipart_procecssor {
public:
	/**
	 * ctor
	 */
	multipart_procecssor(const request &input, 
	    response &output, imultipart_handler &handler);

	/**
	 * Core routine to process the multipart data
	 */
	void process();

private:
	const request &input_;
	response &output_;
	imultipart_handler &handler_;

	// disable copy:
	multipart_procecssor(const multipart_procecssor &other);
	multipart_procecssor &operator = (const multipart_procecssor &other);
};

#endif
