# PROVIDE: isi_cpool_d_api
# REQUIRE: isi_util
# REQUIRE: isi_cpool_d_common
# REQUIRE: isi_licensing
# REQUIRE: isi_pools_fsa_gcfg

ISI_TOP=	../..

.PATH:		${.CURDIR:H}/isi_cpool_d_common

LIB_CXX=	isi_cpool_d_api
SHLIB_MAJOR=	1
SHLIB_MINOR=	0
MAN=

CXXFLAGS+=	${LOCAL_CFLAGS}

SRCS+=		client_task_map.cpp
SRCS+=		cpool_api.cpp
SRCS+=		cpool_message.cpp

LIBADD+=	ifs
LIBADD+=	isi_util
LIBADD+=	isi_cpool_d_common
LIBADD+=	isi_cpool_config
LIBADD+=	isi_gconfig
LIBADD+=	isi_ilog
LIBADD+=	isi_licensing
LIBADD+=	isi_pools_fsa_gcfg
LIBADD+=	isi_util_cpp

CHECK_SRCS+=	check_client_task_map.cpp
CHECK_SRCS+=	check_cpool_api.cpp
CHECK_SRCS+=	check_dedupe_interop.cpp
CHECK_SRCS+=	check_cpool_message.cpp
CHECK_SRCS+=	unit_test_helpers.cpp

CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_job -lisi_job
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_sstore -lisi_sstore
CHECK_LIBS+=    -L${ISI_OBJ}/lib/isi_cpool_d_api -lisi_cpool_d_api
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_ufp -lisi_ufp
CHECK_LIBS+=	-lcrypto

.include "${ISI_TOP}/isi.lib.mk"
