# PROVIDE: isi_cpool_cbm
# REQUIRE: isi_cpool_security
# REQUIRE: isi_cloud_api
# REQUIRE: isi_util_cpp
# REQUIRE: isi_cpool_config
# REQUIRE: isi_pools_fsa_gcfg
# REQUIRE: isi_proxy
# REQUIRE: isi_cpool_d_common
# REQUIRE: isi_cpool_d_api
# REQUIRE: isi_snapshot

ISI_TOP=	../..


LIB_CXX=	isi_cpool_cbm
SHLIB_MAJOR=	1
SHLIB_MINOR=	0
MAN=

.PATH: ${.CURDIR} ${.CURDIR}/io_helper

CFLAGS+=	-DISI_CBM_REMOVE_COLONS_FROM_LINS

CXXFLAGS+=	-std=c++11 -O0

CXXFLAGS+=	${LOCAL_CFLAGS}

LDFLAGS+=	${LOCAL_LDFLAGS}
LDADD+=		-lprotobuf

LIBADD+=	ifs
LIBADD+=	isi_celog
LIBADD+=	isi_config
LIBADD+=	isi_cloud_api
LIBADD+=	isi_cloud_common
LIBADD+=	isi_cpool_config
LIBADD+=	isi_cpool_d_api
LIBADD+=	isi_cpool_d_common
LIBADD+=	isi_cpool_security
LIBADD+=	isi_cpool_stats
LIBADD+=	isi_domain
LIBADD+=	isi_ilog
LIBADD+=	isi_licensing
LIBADD+=	isi_pools_fsa_gcfg
LIBADD+=	isi_proxy
LIBADD+=	isi_sbtree
LIBADD+=	isi_snapshot
LIBADD+=	isi_ufp
LIBADD+=	isi_util
LIBADD+=	isi_util_cpp

LDADD+=		-lcurl
LDADD+=		-lssl
LDADD+=		-lcrypto
LDADD+=		-lpthread
LDADD+=		-lz

DPADD+=		${LIBSSL} ${LIBCRYPTO} ${LIBPTHREAD}

#CBM_DIR=	${ISI_TOP}/lib/isi_cpool_cbm
#isi_cbm_error_enum_fmt.c: ${CBM_DIR}/isi_cbm_error.h
#	python ${CBM_DIR}/isi_cbm_error_gen.py --base ${CBM_DIR}

SRCS=
SRCS+=		azure_io_helper.cpp
SRCS+=		compressed_istream.cpp
SRCS+=		compress_filter.cpp
SRCS+=		checksum_cache.cpp
SRCS+=		checksum_istream.cpp
SRCS+=		checksum_filter.cpp
SRCS+=		sparse_area_filter.cpp
SRCS+=		encrypt_ctx.cpp
SRCS+=		encrypted_istream.cpp
SRCS+=		encrypt_filter.cpp
SRCS+=		headered_istream.cpp
SRCS+=		isi_cbm_access.cpp
SRCS+=		isi_cbm_account_util.cpp
SRCS+=		isi_cbm_account_statistics.cpp
SRCS+=		isi_cbm_archive.cpp
SRCS+=		isi_cbm_cache.cpp
SRCS+=		isi_cbm_cache_iterator.cpp
SRCS+=		isi_cbm_coi.cpp
SRCS+=		isi_cbm_coi_sbt.c
SRCS+=		isi_cbm_csi.cpp
SRCS+=		isi_cbm_data_stream.cpp
SRCS+=		isi_cbm_error.c
SRCS+=		isi_cbm_error_enum_fmt.c
SRCS+=		isi_cbm_error_util.cpp
SRCS+=		isi_cbm_file.cpp
SRCS+=		isi_cbm_gc.cpp
SRCS+=		isi_cbm_id.cpp
SRCS+=		isi_cbm_invalidate.cpp
SRCS+=		isi_cbm_index_types.cpp
SRCS+=		isi_cbm_ioh_base.cpp
SRCS+=		isi_cbm_ioh_creator.cpp
SRCS+=		isi_cbm_mapentry_iterator.cpp
SRCS+=		isi_cbm_mapinfo_impl.cpp
SRCS+=		isi_cbm_mapinfo_store_sbt.cpp
SRCS+=		isi_cbm_mapper.cpp
SRCS+=		isi_cbm_ooi.cpp
SRCS+=		isi_cbm_pack_unpack.cpp
SRCS+=		isi_cbm_policyinfo.cpp
SRCS+=		isi_cbm_purge.cpp
SRCS+=		isi_cbm_read.cpp
SRCS+=		isi_cbm_recall.cpp
SRCS+=		isi_cbm_restore_coi.cpp
SRCS+=		isi_cbm_snap_diff.cpp
SRCS+=		isi_cbm_scoped_coid_lock.cpp
SRCS+=		isi_cbm_scoped_flock.cpp
SRCS+=		isi_cbm_scoped_ppi.cpp
SRCS+=		isi_cbm_snapshot.cpp
SRCS+=		isi_cbm_sparse.cpp
SRCS+=		isi_cbm_stream.cpp
SRCS+=		isi_cbm_sync.cpp
SRCS+=		isi_cbm_util.cpp
SRCS+=		isi_cbm_wbi.cpp
SRCS+=		isi_cbm_write.cpp
SRCS+=		md5_filter.cpp
SRCS+=		range_filter.cpp
SRCS+=		ran_io_helper.cpp
SRCS+=		header_ostream.cpp
SRCS+=		isi_cbm_init.cpp
SRCS+=		s3_io_helper.cpp
SRCS+=		ufp_init.c
SRCS+=		ecs_io_helper.cpp
SRCS+=		google_xml_io_helper.cpp
SRCS+=		isi_cbm_file_versions.pb.cc
SRCS+=		isi_cbm_file_versions.cpp

INCS+=		isi_cbm_file_versions.pb.h
INCSDIR=	${INCLUDEDIR}/isi_cpool_cbm

CHECK_SRCS+=	check_access.cpp
CHECK_SRCS+=	check_cacheinfo.cpp
CHECK_SRCS+=	check_cbm_common.cpp
CHECK_SRCS+=	check_cbm_error.c
CHECK_SRCS+=	check_cbm_error_util.cpp
CHECK_SRCS+=	check_cbm_file.cpp
CHECK_SRCS+=	check_cbm_policyinfo.cpp
CHECK_SRCS+=	check_coi.cpp
CHECK_SRCS+=	check_coi_sbt.c
CHECK_SRCS+=	check_compression_filter.cpp
CHECK_SRCS+=	check_io_helper.cpp
CHECK_SRCS+=	check_mapper.cpp
CHECK_SRCS+=	check_ooi.cpp
CHECK_SRCS+=	check_recall.cpp
CHECK_SRCS+=	check_stub.cpp
CHECK_SRCS+=	check_sync.cpp
#CHECK_SRCS+=	check_encrypt.cpp
CHECK_SRCS+=	check_cacheio.cpp
CHECK_SRCS+=	check_worm.cpp
CHECK_SRCS+=	isi_cbm_test_util.cpp
CHECK_SRCS+=	check_pause_resume.cpp
CHECK_SRCS+=	check_purge.cpp
CHECK_SRCS+=	check_invalidate.cpp
CHECK_SRCS+=	check_restore_coi.cpp
CHECK_SRCS+=	check_scoped_ppi.cpp
CHECK_SRCS+=	check_streaming.cpp
CHECK_SRCS+=	check_cbm_snap.cpp
CHECK_SRCS+=	check_cbm_snap_diff.cpp
CHECK_SRCS+=	check_gc_checkpointing.cpp
CHECK_SRCS+=	check_gc.cpp
CHECK_SRCS+=	check_cbm_account_statistics.cpp
CHECK_SRCS+=	isi_cbm_test_util_ads.cpp
CHECK_SRCS+=	check_cbm_scoped_flock.cpp
CHECK_SRCS+=	check_cbm_versions.cpp
CHECK_SRCS+=	check_time_based_index_entry.cpp
CHECK_SRCS+=	check_time_based_index.cpp
CHECK_SRCS+=	check_times.cpp
CHECK_SRCS+=	check_snapshot_restrictions.cpp
CHECK_SRCS+=	check_sparse.cpp
CHECK_SRCS+=	check_hardening.cpp


CHECK_LIBS+=	-L$(ISI_OBJ)/lib/isi_cpool_security -lisi_cpool_security
CHECK_LIBS+=	-L$(ISI_OBJ)/lib/isi_cpool_config -lisi_cpool_config
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_gconfig -lisi_gconfig
CHECK_LIBS+=	-L$(ISI_OBJ)/lib/isi_domain -lisi_domain
CHECK_LIBS+=	-L$(ISI_OBJ)/lib/isi_util -lisi_util
CHECK_LIBS+=	-lpthread
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_cpool_d_api -lisi_cpool_d_api
CHECK_LIBS+=	-lisi_flexnet -lisi_inx_addr

NO_WERROR=	bug 149963

.include "${ISI_TOP}/isi.lib.mk"

isi_cbm_file_versions.pb.h isi_cbm_file_versions.pb.cc: \
	isi_cbm_file_versions.proto
	${PROTOC} -I=${.CURDIR} --cpp_out=. \
		${.CURDIR}/isi_cbm_file_versions.proto

clean:	clean_proto

clean_proto:
	rm -f *.pb.h *.pb.cc
