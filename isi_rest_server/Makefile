# PROVIDE: isi_rest_server
# REQUIRE: isi_config
# REQUIRE: isi_flexnet
# REQUIRE: isi_ilog
# REQUIRE: isi_json
# REQUIRE: isi_licensing
# REQUIRE: isi_lwconfig
# REQUIRE: isi_ntoken
# REQUIRE: isi_priv
# REQUIRE: isi_priv_db
# REQUIRE: isi_qpl
# REQUIRE: isi_thread
# REQUIRE: isi_util
# REQUIRE: isi_ufp
# REQUIRE: isi_util_cpp
# REQUIRE: isi_version
# REQUIRE: isi_rest_client

ISI_TOP=	../..


LIB_CXX=	isi_rest_server
SHLIB_MAJOR=	1
SHLIB_MINOR=	0
MAN=

CFLAGS+=	${LOCAL_CFLAGS}
CFLAGS+=	${FSP_CFLAGS} # needed for QPL

LDFLAGS+=	${LOCAL_LDFLAGS}
LDFLAGS+=	${FSP_LDFLAGS}

LIBADD+=	isi_config
LIBADD+=	isi_flexnet
LIBADD+=	isi_ilog
LIBADD+=	isi_json
LIBADD+=	isi_licensing
LIBADD+=	isi_lwconfig
LIBADD+=	isi_ntoken
LIBADD+=	isi_priv
LIBADD+=	isi_priv_db
LIBADD+=	isi_qpl
LIBADD+=	isi_thread
LIBADD+=	isi_ufp
LIBADD+=	isi_util
LIBADD+=	isi_util_cpp
LIBADD+=	isi_version
LIBADD+=	isi_rest_client

LDADD+=		-ljsoncpp
LDADD+=		-lcrypto
LDADD+=		-lfcgi
LDADD+=		-lboost_system
LDADD+=		-lboost_filesystem
# QPL needs this
LDADD+=		-llwbase

SRCS=
SRCS+=		api_content.cpp
SRCS+=		api_error.cpp
SRCS+=		api_headers.cpp
SRCS+=		api_utils.cpp
SRCS+=		base64enc.cpp
SRCS+=		fcgi_helpers.cpp
SRCS+=		file_utils.cpp
SRCS+=		handler_doc.cpp
SRCS+=		json_sorter.cpp
SRCS+=		license_info.cpp
SRCS+=		method_serializer.cpp
SRCS+=		method_stats.cpp
SRCS+=		multipart_processor.cpp
SRCS+=		priv_filter.cpp
SRCS+=		request.cpp
SRCS+=		request_thread.cpp
SRCS+=		response.cpp
SRCS+=		rest_server_ufp.c
SRCS+=		resume_token.cpp
SRCS+=		scoped_settcred.cpp
SRCS+=		uri_handler.cpp
SRCS+=		uri_manager.cpp
SRCS+=		uri_mapper.cpp
SRCS+=		auto_version_handler.cpp
SRCS+=		audit_request.cpp
SRCS+=		soa_handler.cpp
SRCS+=		fastcgi_soa_handler.cpp
SRCS+=		environmental_criteria.cpp

CHECK_SRCS+=	check_api_error.cpp
CHECK_SRCS+=	check_api_utils.cpp
CHECK_SRCS+=	check_license_info.cpp
CHECK_SRCS+=	check_priv_filter.cpp
CHECK_SRCS+=	check_request_thread.cpp
CHECK_SRCS+=	check_resume_token.cpp
CHECK_SRCS+=	check_scoped_settcred.cpp
CHECK_SRCS+=	check_thread.cpp
CHECK_SRCS+=	check_uri_mapper.cpp
CHECK_SRCS+=	check_auto_version_handler.cpp

CHECK_LIBS+=	-lpthread
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_ntoken -lisi_ntoken
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_priv_db -lisi_priv_db


.include "${ISI_TOP}/isi.lib.mk"
