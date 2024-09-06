ISI_TOP=	../../../..

.include "${ISI_TOP}/bin/isi_migrate/local/Makefile.tsm"

PROG=		isi_linmap_mod
SRCS=		linmap_mod.c
BINDIR=		/usr/bin
MAN=
NO_PROFILE=	1
CFLAGS+=	-DPROGNAME=\"${PROG}\"

LDFLAGS+=       ${LOCAL_LDFLAGS}

LIBADD+=	isi_migrate
LIBADD+=	isi_sbtree
LIBADD+=	isi_util

.include "${ISI_TOP}/isi.prog.mk"
