
#ifndef __MD5_FILTER__
#define __MD5_FILTER__

#include <isi_cloud_api/isi_cloud_api.h>
#include <openssl/md5.h>

using namespace isi_cloud;

class md5_filter : public odata_stream {
public:
	md5_filter(odata_stream &out);
	~md5_filter() {}
	inline const std::string &get_md5() {
		if (!md5_ready_)
			final();

		return md5_;
	}

	virtual size_t write(void *buff, size_t len);

	virtual bool reset() { return false; }

private:
	void final();

private:
	odata_stream &out_;

	std::string md5_;
	MD5_CTX ctx_;
	bool md5_ready_;
};

#endif
