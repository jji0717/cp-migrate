/*
 * Supporting functions to send HTTP requests
 */
#include <isi_rest_server/request_thread.h>

class test_istream: public idata_stream
{
public:
	virtual size_t read(void *buff, size_t len){return 0;}
	void get_error(std::string &error_info){}
};

class test_ostream: public odata_stream
{
public:
	virtual void write_data(const void *buff, size_t len){}
	virtual void write_from_fd(int f, size_t n){}
	virtual void write_header(const response &rsp){}
};

// for testing use
class test_json_istream: public idata_stream
{
public:
	test_json_istream(const std::string &content)
	    : total_size(content.length()), read_size(0), content_(content)
	{
	}

	virtual size_t read(void *buff, size_t len) {
		ASSERT(len + read_size <= total_size);
		strncpy((char *)buff, content_.c_str() + read_size, len);
		return len;
	}
	void get_error(std::string &error_info) {}

	const std::string &get_content() { return content_; }

	size_t get_size() { return total_size; }

private:
	size_t total_size;
	size_t read_size;
	std::string content_;

};

// for testing use
class test_json_ostream: public odata_stream
{
public:
	test_json_ostream() : total_size(0) {}

	virtual void write_data(const void *buff, size_t len) {
		total_size += len;
		content_.append((const char *) buff, len);
	}
	virtual void write_from_fd(int f, size_t n) {}

	virtual void write_header(const response &rsp) {
		// reset
		content_.clear();
		total_size = 0;
	}

	const std::string &get_content() { return content_; }
	size_t get_size() { return total_size; }

private:
	size_t total_size;
	std::string content_;

};

class test_request_helper
{
public:
	/* 
	 * Method to send a request and return the response
	 */
	void test_send_request(const std::string &method,
	    const std::string &uri, const std::string &store_type,
	    const std::string &args, const api_header_map &headers,
	    response &output);

	/*
	 * Method to send a request with stream and credential
	 */
	void test_send_request_ext(const std::string &method,
	    const std::string &uri, const std::string &store_type,
	    const std::string &args, const api_header_map &headers,
	    size_t content_len, int user_fd, const std::string &remote_addr,
	    idata_stream &strm, response &output);

	/*
	 * Method to get remote ip (loopback) based on IP family
	 */
	std::string get_remote_ip();
};
