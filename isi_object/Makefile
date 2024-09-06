# PROVIDE: isi_object
# REQUIRE: ifs
# REQUIRE: isi_acl
# REQUIRE: isi_config
# REQUIRE: isi_domain
# REQUIRE: isi_gconfig
# REQUIRE: isi_ilog
# REQUIRE: isi_licensing
# REQUIRE: isi_ntoken
# REQUIRE: isi_persona
# REQUIRE: isi_snapshot
# REQUIRE: isi_sstore
# REQUIRE: isi_ufp
# REQUIRE: isi_util

ISI_TOP=	../..
LIB=		isi_object
SHLIB_MAJOR=	1
SHLIB_MINOR=	0
MAN=

CFLAGS+=	-O0 -g

SRCS+=		bucket_common.c		\
		common.c 		\
		fs_op_utils.c          \
		genobj_common.c		\
		iobj_hash.c		\
		iobj_snapshot.c		\
		iobj_licensing.c	\
		ns_bucket.c		\
		ns_object.c		\
		ns_store.c		\
		object_common.c		\
		ob_bucket.c		\
		ob_object.c		\
		ob_store.c		\
		ostore.c		\
		ostore_error.c 		\
		genobj_acl.c		


LIBADD+=	ifs
LIBADD+=	isi_acl
LIBADD+=	isi_config
LIBADD+=	isi_domain
LIBADD+=	isi_gconfig
LIBADD+=	isi_ilog
LIBADD+=	isi_licensing
LIBADD+=	isi_ntoken
LIBADD+=	isi_persona
LIBADD+=	isi_snapshot
LIBADD+=	isi_ufp
LIBADD+=	isi_util
LIBADD+=	isi_sstore
LDADD+=		-lpthread -lxml2
INCSDIR=	${INCLUDEDIR}/isi_object

INCS=		ostore.h common.h genobj_acl.h iobj_ostore_protocol.h \
		iobj_snapshot.h fs_op_utils.h

# only if we want to install on target
#INCLUDEDIR=	${INCLUDEDIR}/isi_ostore.h

### HACK: for now src/isilon/test is unable to find anything in ${ISI_OBJECT}.
### Installing our generated file avoids that.
INCS+=		ostore_gcfg.h

#
# Gconfig.
#  

GCFG_SRCS+=	ostore.gcfg 

#CHECK_SRCS+=	check_bucket.c		\
#		check_bucket_threads.c	\
#		check_common.c 		\
#		check_licensing.c	\
#		check_ns_bucket.c	\
#		check_ns_object.c	\
#		check_ns_store.c 	\
#		check_ns_prohibited.c 	\
#		check_object.c		\
#		check_object_threads.c	\
#		check_store.c 		\
#		check_utils.c

CHECK_SRCS+=	check_ns_bucket.c	\
		check_ns_prohibited.c 	\
		check_ns_store.c 	\
		check_ns_object.c	\
		check_utils.c

CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_gconfig -lisi_gconfig 
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_licensing -lisi_licensing
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_ilog -lisi_ilog
CHECK_LIBS+=	-L${ISI_OBJ}/lib/isi_acl -lisi_acl
CHECK_LIBS+=	-lcrypto -lrt

.include "${ISI_TOP}/isi.gcfg.pre.mk"
.include "${ISI_TOP}/isi.lib.mk"
.include "${ISI_TOP}/isi.gcfg.post.mk"

