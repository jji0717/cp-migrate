#ifndef _MSG_INTERNAL_H
#define _MSG_INTERNAL_H

#include "isirep.h"

#define BUFSIZE (1024 * 2048 + MAXNAMELEN + 1 + 52 + CHECKSUM_SIZE)
#define MAX_MSG_FDM_DATA (1024 * 2048)
#define IBUFSIZE 65536
#define KEEPALIVE 300 /* 5 minutes. */
#define MAX_MSG_MEMBERS 48

struct mesgtab {
	enum messages	msg_id;
	char	*name;
	char	*pack; /* s=string, u=unsigned, U=u_int64_t, d=data */
	char	*members[MAX_MSG_MEMBERS];
	int	size;
};

struct riptmesgtab {
	enum ript_messages	msg_id;
	char	*name;
};

struct ript_message_fields_tab {
	enum ript_message_fields	field;
	char	*name;
};

struct mesgtab *msg_get_mesg_tab(enum messages msg_id);

#endif
