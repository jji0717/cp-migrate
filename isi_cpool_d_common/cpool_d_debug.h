#ifndef __ISI_CPOOL_D_DEBUG_H__
#define __ISI_CPOOL_D_DEBUG_H__

#include <stdio.h>

#include <isi_cpool_d_common/hsbt.h>
#include <isi_util/isi_format.h>

#define P(format, ...) \
{ \
	struct fmt FMT_INIT_CLEAN(fmt); \
	fmt_print(&fmt, "[%s:%04d] " format " \n", \
	    __FUNCTION__, __LINE__, ##__VA_ARGS__); \
	printf("%s", fmt_string(&fmt)); \
	fflush(NULL); \
}

#define Pt(format, ...) \
{ \
	struct fmt FMT_INIT_CLEAN(fmt); \
	fmt_print(&fmt, "[%p:%s:%04d] " format " \n", \
	    pthread_self(), __FUNCTION__, __LINE__, ##__VA_ARGS__); \
	printf("%s", fmt_string(&fmt)); \
	fflush(NULL); \
}

void
print_buffer_as_hex(const void *buffer, size_t buffer_size, struct fmt &fmt);
void
print_hsbt_bulk_ent(const struct hsbt_bulk_entry &hsbt_ent, struct fmt &fmt);
void
print_sbt_bulk_ent(const struct sbt_bulk_entry &sbt_ent, struct fmt &fmt);

#endif // __ISI_CPOOL_D_DEBUG_H__
