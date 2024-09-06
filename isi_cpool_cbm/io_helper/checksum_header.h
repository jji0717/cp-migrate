
#ifndef __CHECKSUM_HEADER_H__
#define __CHECKSUM_HEADER_H__

#include <stdio.h>

#include <isi_cpool_security/cpool_protect.h>
#include <vector>

typedef struct {
	char data[MAX_CHECKSUM_SIZE];
} checksum_t;

typedef std::vector<checksum_t> checksum_header;

inline void dump_cksum(const checksum_t &cksm)
{
	char data[MAX_CHECKSUM_SIZE * 2 + 1];
	bzero(data, sizeof(data));

#define HEX_TO_CHAR(h) ((h) >= 10 ? ((h) + 'a' - 10) : (h) + '0')

	for (int i = 0; i < MAX_CHECKSUM_SIZE; ++i) {
		data[i * 2] = HEX_TO_CHAR(cksm.data[i] & 0x0F);
		data[i * 2 + 1] = HEX_TO_CHAR(cksm.data[i] >> 4);
	}

	printf("\t %s\n", data);
}

inline void trace_cksum(const char *msg, int indx, const checksum_t &cksm)
{
	char data[MAX_CHECKSUM_SIZE * 2 + 1];
	bzero(data, sizeof(data));

#define HEX_TO_CHAR(h) ((h) >= 10 ? ((h) + 'a' - 10) : (h) + '0')

	for (int i = 0; i < MAX_CHECKSUM_SIZE; ++i) {
		data[i * 2] = HEX_TO_CHAR(cksm.data[i] & 0x0F);
		data[i * 2 + 1] = HEX_TO_CHAR(cksm.data[i] >> 4);
	}

	ilog(IL_DEBUG,"%s, index %d: %s\n", msg, indx, data);
}

inline void dump_cksum(const checksum_header &cksm)
{
	for (checksum_header::const_iterator itr = cksm.begin();
	    itr != cksm.end(); ++itr)
		dump_cksum(*itr);
}

inline void trace_cksum_hdr(const char *msg, const checksum_header &cksm)
{
	int indx = 0;
	for (checksum_header::const_iterator itr = cksm.begin();
	    itr != cksm.end(); ++itr) {
		trace_cksum(msg, indx, *itr);
		indx++;
	}
}

#endif
