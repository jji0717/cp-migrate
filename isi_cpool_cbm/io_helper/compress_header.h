
#ifndef __STREAM_HEADER_H__
#define __STREAM_HEADER_H__

#include <ifs/ifs_types.h>

typedef struct compress_header
{
	enum {
		NONE		= 0x00,
		COMPRESSED	= 0x01,
		ENCRYPTED	= 0x02,
		WITHCHKSUM	= 0x04
	};


	// use size_t for alignment
	size_t flag;
	size_t len;

	inline void set_compression(bool compress)
	{
		if (compress)
			flag |= COMPRESSED;
		else
			flag &= ~COMPRESSED;
	}

	inline bool is_compressed()
	{
		return flag & COMPRESSED;
	}

	inline void set_encryption(bool encrypt)
	{
		if (encrypt)
			flag |= ENCRYPTED;
		else
			flag &= ~ENCRYPTED;
	}

	inline bool is_encrypted()
	{
		return flag & ENCRYPTED;
	}

	inline void set_with_chksum(bool with_chksum)
	{
		if (with_chksum)
			flag |= WITHCHKSUM;
		else
			flag &= ~WITHCHKSUM;
	}

	inline bool has_chksum()
	{
		return flag & WITHCHKSUM;
	}

	inline char *data_ptr()
	{
		return (char *) this + sizeof(this);
	}


} compress_header, *compress_header_p;


#endif
