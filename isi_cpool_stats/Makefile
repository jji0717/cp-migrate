# PROVIDE: isi_cpool_stats
# REQUIRE: isi_ilog
# REQUIRE: isi_util

ISI_TOP=	../..


LIB_CXX=	isi_cpool_stats
SHLIB_MAJOR=	1
SHLIB_MINOR=	0
MAN=

CFLAGS+=	${LOCAL_CFLAGS}

SRCS+=		cpool_stat_manager.cpp
SRCS+=		cpool_stat_key.cpp
SRCS+=		cpool_stat_persistence_manager.cpp
SRCS+=		cpool_stat.cpp
SRCS+=		isi_cpool_stats_api_cb.cpp
SRCS+=		cpool_stat_read_write_lock.cpp

LDFLAGS+=	${LOCAL_LDFLAGS}

LIBADD+=	isi_ilog
LIBADD+=	isi_util
LIBADD+=	isi_util_cpp

LDADD+=		-lsqlite3

CHECK_SRCS+=	check_cpool_stat.cpp
CHECK_SRCS+=	check_shared_memory.cpp
CHECK_SRCS+=	check_read_write_locks.cpp
CHECK_SRCS+=	check_persistence.cpp
CHECK_SRCS+=	check_stat_manager.cpp

CHECK_LIBS+=	${LDFLAGS}
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_cpool_stats -lisi_cpool_stats
CHECK_LIBS+=	-lsqlite3 -lcrypto -lpthread

.include "${ISI_TOP}/isi.lib.mk"
