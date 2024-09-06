#ifndef FCGI_HELPERS_H_
#define FCGI_HELPERS_H_

#include <fcgiapp.h>

#include <isi_rest_server/request.h>
#include <isi_rest_server/response.h>

class fcgi_request;

class fcgi_istream : public idata_stream {
public:
	fcgi_istream(fcgi_request &f);

	size_t read(void *buff, size_t len);

	void get_error(std::string &error_info);

private:
	fcgi_request &fcgi_;
};

class fcgi_ostream : public odata_stream {
public:
	fcgi_ostream(fcgi_request &fq);

	void write_data(const void *buff, size_t len);

	void write_from_fd(int f, size_t n);

	void write_header(const response &r);

private:
	fcgi_request &fcgi_;
};

class fcgi_request : public FCGX_Request {
public:
	/** Create from server socket @a sock with @a flags
	 * if internal is true this request originated from within the
	 * cluster */
	inline fcgi_request(int sock, int flags, bool internal = false);

	inline ~fcgi_request();

	/** Use FCGX_Finish to finish the request from a threadpool */
	inline void finish();

	/** Use custom cleanup to finish the request from a forked process.
	 *
	 * If @a child is false, clean up memory and close sockets without
	 * disconnecting the stream.
	 * If @a child is true, clean up memory and properly disconnect and
	 * close the stream.
	 *
	 * This function is mostly a deconstruction of FCGX_Finish_r and
	 * FCGX_Free.
	 *
	 * Assumes non-persistent connections.
	 */
	void finish(bool child);

	/** Create an api::request from this.
	 *
	 * References this, which must outlive the created object.
	 *
	 * @return NULL if the conversion could not take place
	 */
	request *api_request();//////将原生的url里面的信息,解析,包装成object_d可以使用request

	/** Get access to input stream */
	inline fcgi_istream &get_input_stream();

	/** Get access to output stream */
	inline fcgi_ostream &get_output_stream();

	/** Indicates whether the request came from the backend network or not */
	bool internal;

private:
	fcgi_istream istream_;
	fcgi_ostream ostream_;

	FCGX_Request* redirect_;
};

/*  _       _ _            
 * (_)_ __ | (_)_ __   ___ 
 * | | '_ \| | | '_ \ / _ \
 * | | | | | | | | | |  __/
 * |_|_| |_|_|_|_| |_|\___|
 */

fcgi_request::fcgi_request(int sock, int flags, bool internal)
	: internal(internal), istream_(*this), ostream_(*this), redirect_(this)
{
	FCGX_InitRequest(this, sock, flags); /////sock = FCGX_OpenSocket("/var/run/isi_object_d.sock");
}


fcgi_request::~fcgi_request()
{
	FCGX_Free(redirect_, true);
}

void
fcgi_request::finish()
{
	FCGX_Finish_r(this);
}

fcgi_istream &
fcgi_request::get_input_stream()
{
	return istream_;
}

fcgi_ostream &
fcgi_request::get_output_stream()
{
	return ostream_;
}

#endif
