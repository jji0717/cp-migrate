#include <string.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <ifs/dfm/dfm_on_disk.h>

#include "isirep.h"
#include "treewalk.h"

static int OVERRIDESIZE = -1;

/*
 * Reset the list but keep the initial directory header
 */
void
reset_list_head(struct filelist *list)
{
	char *index = NULL;
	ASSERT(list->list != NULL);
	index = strchr(list->list, '\n');
	ASSERT(index != NULL);
	*index = '\0';
	list->used = strlen(list->list) + 1;
	list->elems = 0;
	*index = '\n';
}

/*
 * reset list and add 1-st element - dir name (for deleted-files list)
 * dir should be in UTF-8, list size shoul be enough
 */
void
init_list_deleted(struct filelist *list, char* dir)
{
	int len;
	log(TRACE, "init list deleted for %s", dir);

	/*
	 * If we do init due to list getting full, then 
	 * initialize the list keeping the initial directory
	 * entry
	 */
	if (dir == NULL) {
		reset_list_head(list);
		goto out;
	}

	len = strlen(dir);

	if (!list->list || len >= list->size)
		list->list = realloc(list->list, list->size = len + 1 > 1024 ?
		    len + 1 : 1024);

	memcpy(list->list, dir,  len);
	list->used = len + 1;
	list->list[list->used - 1] = '\n';
	list->elems = 0;

out:
	return;
}

void
init_list_deleted_dst(struct filelist *list, char *dir, char *clu_name)
{
	int len;
	log(TRACE, "init list deleted on %s for %s", clu_name, dir);

	/*
	 * If we do init due to list getting full, then 
	 * initialize the list keeping the initial directory
	 * entry
	 */
	if (dir == NULL || clu_name == NULL) {
		reset_list_head(list);
		goto out;
	}

	len = strlen(dir);
	len += strlen(clu_name);
#define FORMAT_STR_LEN 5
	if (!list->list || (len + FORMAT_STR_LEN) >= list->size) {
		list->size = (len + FORMAT_STR_LEN) > 1024 ?
				(len + FORMAT_STR_LEN): 1024;
		list->list = realloc(list->list, list->size);
	}
	sprintf(list->list, "//%s:%s", clu_name, dir);
	len = strlen(list->list);
	list->list[len] = '\n';	
	list->used = len + 1;
#undef FORMAT_STR_LEN

	list->elems = 0;
	
out:
	return;
}

void
dump_listmsg_to_file(int fd,  char* data, unsigned size)
{
	/* size = 1 means that the list was empty */
	if (size > 1) {
		write(fd, data, size);
	}
}

static void
listsend(struct filelist *list, int fd, int mode, bool continued, bool last)
{
	struct generic_msg m;
	int size;
	int offset;

	log(TRACE, "listsend");

	if (!list)
		goto out;

	/* mode LIST_TO_SYNC is for messages from pworker to sworker (for
	 * target deletes) and includes list range and other details. the other
	 * modes are for passing around generic file lists for accounting, etc.
	 * and use the older list message format */

	if (mode == LIST_TO_SYNC) {
		log(DEBUG, "sending %d entries, range %x-%x, continued: %d, "
		    "last: %d", list->elems, list->range_begin, list->range_end,
		    continued, last);

		m.head.type = LIST_MSG2;
		m.body.list2.range_begin = list->range_begin;
		m.body.list2.range_end = list->range_end;
		m.body.list2.continued = continued;
		m.body.list2.last = last;
		m.body.list2.used = list->used;

		/* offset pointer to correct location in generic_send_buf */
		offset = sizeof(m.body.list2.range_begin) +
			 sizeof(m.body.list2.range_end) +
			 sizeof(m.body.list2.continued) +
			 sizeof(m.body.list2.last);
		m.body.list2.list = (char *)get_data_offset(fd, &size) + offset;
		size -= offset;

		ASSERT(size >= list->used);

		if (list->used)
			memcpy(m.body.list2.list, list->list, list->used);
	} else {
		m.head.type = LIST_MSG;
		m.body.list.last = last;
		
		/* offset pointer to correct location in generic_send_buf */
		offset = sizeof(m.body.list.last);
		m.body.list.list = (char *)get_data_offset(fd, &size) + offset;
		size -= offset;

		ASSERT(size >= list->used);
		m.body.list.used = list->used;
		if (list->used) {
			memcpy(m.body.list.list, list->list, list->used);
		} else {
			/* Value of 1 tells receiver the list is empty */
			m.body.list.used = 1;
		}
	}

	if ((mode == LIST_TO_SYNC) || (list->elems > 0))
		msg_send(fd, &m);

	list->used = 0;
	list->elems = 0;

out:
	return;
}

void
listreset(struct filelist *list)
{
	log(TRACE, "listreset");
	list->used = 0;
	list->elems = 0;
	list->range_begin = dfm_key2hash(DIR_SLICE_MIN);
	list->range_end = dfm_key2hash(DIR_SLICE_MIN);
}

void
listadd(struct filelist *list, const char *name, enc_t enc,
    u_int64_t cookie, int mode, int fd, char *dir)
{
	static char ename[MAXNAMELEN + 1];
	int namelen = strlen(name);
	int len = namelen + 1 + sizeof(u_int16_t);
	bool continued;
	unsigned hash = dfm_key2hash(cookie);
	int maxsize = (OVERRIDESIZE > 0) ? OVERRIDESIZE : MAXLISTSIZE;

	log(TRACE, "listadd");
	log(DEBUG, "add %s to list", name);

	if ((list->list && list->used + len >= maxsize)) {
		/* if the hash range of the message being sent overlaps hash
		 * range of the next message, it is a special case where the
		 * sworker needs both this list and the next one before it can
		 * be certain a file with this hash value should be deleted */
		continued = (list->range_end == hash);

		listsend(list, fd, mode, continued, false);

		/* if the file being added has a different cookie than that of
		 * the last file in the list we just sent, then we just start
		 * the new range one past the end of the old one. if the cookie
		 * is the same as the last file in the list, we start the range
		 * at the cookie */
		list->range_begin = MIN(list->range_end + 1, hash);
	}

	/* update list hash range to include the new file */
	list->range_end = hash;

	if (!list->list || list->used + len >= list->size) {
		list->list = realloc(list->list,
		    list->size = list->size != 0 ? list->size * 2: 1024);
		ASSERT(list->list != NULL);
	}

	if (!list->used) {
		switch (mode) {
		case LIST_TO_LOG:
			init_list_deleted(list, dir);
			break;
		case LIST_TO_SLOG:
			init_list_deleted_dst(list, dir, NULL);
			break;
		default:
			break;
		}
	}

 	/* if encoding supported add it to list if mode == LIST_TO_SYNC or
 	 * convert to UTF-8 if mode == LIST_TO_LOG */
	if (mode == LIST_TO_SYNC) {
		/* XXX alignment ? */
		*(u_int16_t *)(list->list + list->used) = enc;
		list->used += sizeof(u_int16_t);
	}

	list->elems++;
	if (mode == LIST_TO_SYNC) {
		memcpy(list->list + list->used, name, namelen + 1);
		list->used += namelen + 1;
	} else if (enc == ENC_UTF8) {
		memcpy(list->list + list->used, name, namelen);
		list->used += namelen + 1;
		list->list[list->used - 1] = '\n';
	} else {
		/* Need to convert name before add */
		size_t elen = MAXNAMELEN + 1;

		if (enc_conv(name, enc, ename, &elen, ENC_UTF8) == -1) {
			log(DEBUG, "Can't add file to the list %d: failed to "
			    "translate name %s from %s to %s", mode, name,
			    enc_type2name(enc), enc_type2name(ENC_UTF8));
		} else {
			memcpy(list->list + list->used, ename, elen);
			list->used += elen + 1;
			list->list[list->used - 1] = '\n';
		}
	}
}

void
listflush(struct filelist *list, int mode, int fd) {
	log(TRACE, "listflush");
	
	/* when flush is called, we've reached the end of the directory. set
	 * the end hash range to DIR_SLICE_MAX regardless of the last cookie
	 * that was read so that the sworker deletes entries up to the end of
	 * the directory */
	list->range_end = dfm_key2hash(DIR_SLICE_MAX);
	listsend(list, fd, mode, false, true);
}

void
listoverridesize(int override) {
	OVERRIDESIZE = override;
}
