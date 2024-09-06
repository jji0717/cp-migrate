# PROVIDE: isi_object_api
# REQUIRE: isi_acl
# REQUIRE: isi_object
# REQUIRE: isi_rest_server
# REQUIRE: isi_util_cpp


ISI_TOP=	../..


LIB_CXX=	isi_object_api
SHLIB_MAJOR=	1
SHLIB_MINOR=	0
MAN=

.PATH: ${.CURDIR} $(.CURDIR)/oapi

CFLAGS+=	-O0 -g
CFLAGS+=	-I$(ISI_OBJ)/lib
CFLAGS+=	${LOCAL_CFLAGS}
CFLAGS+=	${FSP_CFLAGS}

LDFLAGS+=	${LOCAL_LDFLAGS}

LIBADD+=	ifs
LIBADD+=	isi_acl
LIBADD+=	isi_auth_cpp
LIBADD+=	isi_ilog
LIBADD+=	isi_gconfig
LIBADD+=	isi_object
LIBADD+=	isi_persona
LIBADD+=	isi_rest_server
LIBADD+=	isi_util_cpp
LIBADD+=	isi_util

LDADD+=		-ljsoncpp
LDADD+=		-lcrypto

SRCS=
SRCS+=		oapi_mapper.cpp
SRCS+=		oapi.cpp

# URI implementations
SRCS+=		oapi_snapshot.cpp
SRCS+=		attribute_manager.cpp
SRCS+=		acct_handler.cpp
SRCS+=		bucket_handler.cpp
SRCS+=		common_obj_handler.cpp
SRCS+=		handler_common.cpp
SRCS+=		ns_attr_mgr.cpp
SRCS+=		ns_handler_provider.cpp
SRCS+=		ns_object_handler.cpp
SRCS+=		ns_object_protection.cpp
SRCS+=		ns_object_worm.cpp
SRCS+=		ns_svc_handler.cpp
SRCS+=		object_handler.cpp
SRCS+=		ob_attr_mgr.cpp
SRCS+=		oapi_acct.cpp
SRCS+=		oapi_config.cpp
SRCS+=		oapi_store.cpp
SRCS+=		oapi_genobj.cpp
SRCS+=		oapi_genobj_opener.cpp
SRCS+=		oapi_bucket.cpp
SRCS+=		oapi_object.cpp
SRCS+=		oapi_attrval.cpp
SRCS+=		oapi_acl.cpp
SRCS+=		object_query.cpp
SRCS+=		query_handler.cpp
SRCS+=		store_provider.cpp
SRCS+=		acl_common.cpp

# gconfig

GCFG_SRCS=
GCFG_SRCS+=	oapi.gcfg

INCS=
INCS+=		oapi_gcfg.h

INCSDIR=	${INCLUDEDIR}/isi_object_api

CHECK_SRCS+=	check_ns_object_handler.cpp
CHECK_SRCS+=	check_oapi_store.cpp
CHECK_SRCS+=	check_object_handler.cpp
CHECK_SRCS+=	check_object_query.cpp
CHECK_SRCS+=	check_protection.cpp
CHECK_SRCS+=	test_request_helper.cpp

CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_acl -lisi_acl
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_json -lisi_json
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_util_cpp -lisi_util_cpp
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_object -lisi_object
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_rest_server -lisi_rest_server
CHECK_LIBS+=	-ljsoncpp -lcrypto
CHECK_LIBS+=	-lcrypto -lrt

NO_WERROR=	bug 149923

GCFG_DEPEND_ON_TOOL_UPDATES=1

.include "${ISI_TOP}/isi.gcfg.pre.mk"
.include "${ISI_TOP}/isi.lib.mk"
.include "${ISI_TOP}/isi.gcfg.post.mk"
