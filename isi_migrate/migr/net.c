#include <assert.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/sysctl.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socketvar.h>
#include <isi_gmp/isi_gmp.h>
#include <isi_event/isi_event.h>

#include "isirep.h"
#include "isi_migrate/siq_error.h"

static u_int64_t tracked_recv_netstat = 0;
static u_int64_t tracked_send_netstat = 0;
static int tracked_fd = -1;

struct inx_addr g_dst_addr = {};
struct inx_addr g_src_addr = {};
static int bandwidth = -1;
static int child_count = 0;

void
add_recv_netstat(int fd, uint64_t got)
{
	if (tracked_fd == fd)
		tracked_recv_netstat += got;
}

static inline void
add_send_netstat(int fd, uint64_t got)
{
	if (tracked_fd == fd)
		tracked_send_netstat += got;
}

void
set_tracked_fd(int fd)
{
	tracked_fd = fd;
}

uint64_t
migr_get_recv_netstat(void)
{
	return tracked_recv_netstat;
}

uint64_t
migr_get_send_netstat(void)
{
	return tracked_send_netstat;
}

static void
v4mapped_to_v4(struct inx_addr *addr)
{
	uint32_t tmp = 0;
	if (addr->inx_family == AF_INET6 &&
	    IN6_IS_ADDR_V4MAPPED(&addr->uinx_addr.inx_v6)) {
		tmp = addr->uinx_addr.inx_v6.__u6_addr.__u6_addr32[3];
		memset(addr, 0, sizeof(struct inx_addr));
		addr->inx_family = AF_INET;
		addr->inx_addr4 = tmp;
	}
}

static void
handle_forced_addr(int sock, const struct inx_addr *forced_addr,
    struct isi_error **error_out)
{
	int res;
	int size;
	struct sockaddr *addr = NULL;
	struct sockaddr_in addr4 = { .sin_family = AF_INET };
	struct sockaddr_in6 addr6 = { .sin6_family = AF_INET6 };
	struct isi_error *error = NULL;

	if (forced_addr->inx_family == AF_INET) {
		addr = (struct sockaddr *)&addr4;
		addr4.sin_addr.s_addr = forced_addr->inx_addr4;
		size = sizeof(struct sockaddr_in);
	} else if (forced_addr->inx_family == AF_INET6) {
		addr = (struct sockaddr *)&addr6;
		memcpy(&addr6.sin6_addr.s6_addr, &forced_addr->inx_addr6,
		    sizeof(forced_addr->inx_addr6));
		size = sizeof(struct sockaddr_in6);
	} else
		ASSERT(false);

	res = bind(sock, addr, size);
	if (res == -1) {
		error = isi_siq_error_new(E_SIQ_CONN_TGT,
		    "Error binding socket to local interface: %s",
		    strerror(errno));
		goto out;
	}

out:
	isi_error_handle(error, error_out);
}

/*
 * connect_to
 *
 * Return a socket bound to the target host
 */

int
connect_to(char *hostname, uint16_t port, enum handshake_policy policy,
    uint32_t max_ver, uint32_t *prot_ver, const struct inx_addr *forced_addr,
    bool bw_track, struct isi_error **error_out)
{
	struct addrinfo hints = {};
	struct addrinfo *addrs = NULL;
	struct addrinfo *cur;
	struct timeval timeout;
	struct isi_error *error = NULL;
	int sock = -1;
	int flags = 0;
	int ret = 0;
	int res;
	int sock_errno;
	char portbuf[32];
	bool einprogress_on_connect;
	fd_set fds;
	socklen_t len;
	unsigned null_prot_ver = 0;

	log(TRACE, "connect_to");

	if (prot_ver)
		*prot_ver = 0;
	else
		prot_ver = &null_prot_ver;

	timeout.tv_sec = get_connect_timeout();
	timeout.tv_usec = 0;

	hints.ai_socktype = SOCK_STREAM;
	/* XXX SMS - necessary for local interface binding? 
	hints.ai_flags = AI_PASSIVE; */

	snprintf(portbuf, sizeof(portbuf), "%u", port);
	res = getaddrinfo(hostname, portbuf, &hints, &addrs);
	if (res != 0) {
		error = isi_siq_error_new(E_SIQ_CONN_TGT,
		    "Can't connect to target cluster %s due to hostname "
		    "resolution error: %s", hostname, gai_strerror(res));
		goto out;
	}

	for (cur = addrs; cur != NULL; cur = cur->ai_next) {
		/* cleanup from previous loop */
		einprogress_on_connect = false;
		if (sock != -1) {
			close(sock);
			sock = -1;
		}
		if (error) {
			isi_error_free(error);
			error = NULL;
		}

		sock = socket(cur->ai_family, SOCK_STREAM, 0);
		if (sock == -1) {
			error = isi_siq_error_new(E_SIQ_CONN_TGT,
			    "socket failed: %s", strerror(errno));
			goto out;
		}

		flags = fcntl(sock, F_GETFL);
		if (flags == -1) {
			error = isi_siq_error_new(E_SIQ_CONN_TGT,
			    "fcntl get flags failed: %s", strerror(errno));
			goto out;
		}

		if (!(flags & FNDELAY)) {
		    	res = fcntl(sock, F_SETFL, flags |= FNDELAY);
			if (res == -1) {
			    	error = isi_siq_error_new(E_SIQ_CONN_TGT,
				    "fcntl set non-blocking failed: %s",
				    strerror(errno));
				goto out;
			}
		}

		if (forced_addr)
			handle_forced_addr(sock, forced_addr, &error);
		if (error)
			continue; 

		res = connect(sock, cur->ai_addr, cur->ai_addrlen);
		if (res == -1) {
			/* With this non-blocking socket, EINPROGRESS
			 * indicates the connection may not have comepleted
			 * yet. Will check again for connection in select()
			 * call below. */
			if (errno == EINPROGRESS || errno == EINTR)
				einprogress_on_connect = true;
			else {
				error = isi_siq_error_new(E_SIQ_CONN_TGT,
				    "connect failed: %s", strerror(errno));
				continue;
			}
		}

		FD_ZERO(&fds);
		FD_SET(sock, &fds);
		while (true) {
			res = select(sock + 1, 0, &fds, 0, &timeout);
			if (res == 0) {
				/* Timed out. if select timed out after a failed
				 * connect with EINPROGRESS, most likely that 
				 * indicates that the connect was to an invalid 
				 * address */
				if (einprogress_on_connect)
					error = isi_siq_error_new(
					    E_SIQ_CONN_TGT,
					    "Can't connect to node IP address");
				else
					error = isi_siq_error_new(
					    E_SIQ_CONN_TGT,
					    "select timed out after %ld secs",
					    timeout.tv_sec);
			} else if (res == -1) {
				/* error */
				if (errno == EINTR) {
					/* Need to retry */
					continue;
				} else {
					error = isi_siq_error_new(
					    E_SIQ_CONN_TGT,
					    "select failed: %s", 
					    strerror(errno));
				}
			}
			break;
		}
		if (error) {
			continue;
		}

		len = sizeof(sock_errno);
		res = getsockopt(sock, SOL_SOCKET, SO_ERROR, &sock_errno, &len);
		if (res == -1) {
			error = isi_siq_error_new(E_SIQ_CONN_TGT,
			    "getsockopt failed :%s", strerror(errno));
			continue;
		} else if (sock_errno) {
			if (sock_errno == ECONNREFUSED)
				error = isi_siq_error_new(E_SIQ_CONN_TGT,
				    "Connection refused. Is SyncIQ enabled "
				    "on target?");
			else
				error = isi_siq_error_new(E_SIQ_CONN_TGT,
				    "Socket error: %s", strerror(sock_errno));
			continue;
		}

		/* Clear NDELAY flag */
		res = fcntl(sock, F_SETFL, flags &= ~FNDELAY);
		if (res == -1) {
			error = isi_siq_error_new(E_SIQ_CONN_TGT,
			    "fcntl set blocking failed: %s", strerror(errno));
			goto out;
		}

		/* Attempt protocol handshake with peer */
		res = msg_handshake(sock, max_ver, prot_ver, policy);
		if (res == -1) {
			error = isi_siq_error_new(E_SIQ_CONN_TGT,
			    "msg_handshake failed (%x)", *prot_ver);
			continue;
		} else {
			/* success! */
			ret = sock;
			if (bw_track)
				tracked_fd = sock;
			goto out;
		}
	}

out:
	if (error && (sock != -1)) {
		close(sock);
		sock = -1;
	}

	if (addrs)
		freeaddrinfo(addrs);

	isi_error_handle(error, error_out);
	return sock;
}

int
listen_on(unsigned short port, bool block)
{
	struct sockaddr_in6 sa;
	int s, len, flags, ret;

	s = socket(AF_INET6, SOCK_STREAM, 0);
	if (s == -1) {
		log(ERROR, "%s: socket failed: %s",
		    __func__, strerror(errno));
		goto error;
	}

	if (!block) {
		//Set nonblocking to prevent select/accept race
		flags = fcntl(s, F_GETFL, 0);
		if (flags == -1) {
			log(ERROR, "%s: fcntl(F_GETFL) failed: %s",
			    __func__, strerror(errno));
			goto error;
		}
		ret = fcntl(s, F_SETFL, flags | O_NONBLOCK);
		if (ret == -1) {
			log(ERROR, "%s: fcntl(F_SETFL) failed: %s",
			    __func__, strerror(errno));
			goto error;
		}
	}

	len = 1;
	if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &len, sizeof(len)) != 0) {
		log(ERROR, "%s: setsockopt failed: %s",
		    __func__, strerror(errno));
		goto error;
	}

	bzero(&sa, sizeof(sa));
	sa.sin6_len = sizeof(sa);
	sa.sin6_family = AF_INET6;
	sa.sin6_port = htons(port);
	sa.sin6_addr = in6addr_any;

	if (bind(s, (struct sockaddr *)&sa, sizeof(sa)) != 0) {
		log(ERROR, "%s: bind failed: %s", __func__, strerror(errno));
		goto error;
	}

	if (listen(s, -1) != 0) {
		log(ERROR, "%s: listen filed: %s", __func__, strerror(errno));
		goto error;
	}

	return s;

error:
	if (s != -1)
		close(s);
	return -1;
}

void
sigchld(int signo)
{
}

int
get_child_count()
{
	return child_count;
}

static int lnn_file_fd = -1;
static int lnn_kev_fd = -1;
static struct kevent lnn_kev;

static void
remove_lnn_kevent(void)
{
	if (lnn_kev_fd >= 0) {
		close(lnn_kev_fd);
		lnn_kev_fd = -1;
	}
	if (lnn_file_fd >= 0) {
		close(lnn_file_fd);
		lnn_file_fd = -1;
	}
}

static int
init_lnn_kevent(void)
{
	if (lnn_kev_fd == -1) {
		lnn_kev_fd = kqueue();
		if (lnn_kev_fd == -1) {
			log(ERROR, "%s: failed to init kqueue: %s", __func__,
			    strerror(errno));
			return -1;
		}
	}
	return 0;
}

static int
add_lnn_kevent(char *path)
{
	int ret = -1;

	remove_lnn_kevent();
	//Initialize kqueue fd, if needed
	ret = init_lnn_kevent();
	if (ret == -1) {
		goto out;
	}
	lnn_file_fd = open(path, O_RDONLY);
	if (lnn_file_fd == -1) {
		if (errno == ENOENT) {
			//The file does not exist yet, so set a kevent
			//for the containing directory instead
			lnn_file_fd = open(SIQ_WORKER_POOL_DIR, O_RDONLY);
			if (lnn_file_fd < 0) {
				//Give up
				log(ERROR,
				    "%s: failed to open worker pool dir",
				    __func__);
				goto out;
			}
			EV_SET(&lnn_kev, lnn_file_fd, EVFILT_VNODE,
			    EV_ADD | EV_ENABLE | EV_CLEAR, NOTE_WRITE, 0,
			    NULL);
		} else {
			//Unknown error, bail
			log(ERROR, "%s: failed to open %s", __func__, path);
			goto out;
		}
	} else {
		//Set a kevent on the file
		EV_SET(&lnn_kev, lnn_file_fd, EVFILT_VNODE, EV_ADD |
		    EV_ENABLE |EV_CLEAR, NOTE_DELETE | NOTE_EXTEND |
		    NOTE_WRITE | NOTE_ATTRIB, 0, NULL);
	}

	if (kevent(lnn_kev_fd, &lnn_kev, 1, NULL, 0, NULL) == -1) {
		log(ERROR, "%s: Failed to add kevent for %s %s", __func__, path,
		    strerror(errno));
		goto out;
	}

	ret = 0;

out:
	if (ret < 0 && lnn_file_fd >= 0) {
		close(lnn_file_fd);
		lnn_file_fd = -1;
	}

	return ret;
}

/* Create a lock file by appending ".lock" the given path and locking it */
int
lock_file(char *path, int lock_type)
{
	int lock_fd = -1;
	char *lock_path = NULL;

	ASSERT(lock_type == O_SHLOCK || lock_type == O_EXLOCK);
	ASSERT(path != NULL);
	asprintf(&lock_path, "%s.lock", path);
	lock_fd = open(lock_path, lock_type | O_RDONLY | O_CREAT, 0400);
	if (lock_fd < 0)
		log(ERROR, "Failed to open lock file (%s): %s", strerror(errno),
		    lock_path);

	free(lock_path);
	return lock_fd;
}

static int
read_int_from_file(char *path, int *to_return)
{
	FILE *file;
	int size = 0;
	char *buffer = NULL;
	int res = 0;
	int ret = -1;

	//File contents should only be a single int
	file = fopen(path, "r");
	if (!file) {
		if (errno == ENOENT) {
			//Just set a sane default until the file is written
			ret = 0;
			goto out;
		} else {
			log(ERROR, "%s: Failed to fopen file: %s (%d)",
			    __func__, path, errno);
			goto out;
		}
	}

	fseek(file, 0, SEEK_END);
	size = ftell(file);
	if (size <= 0) {
		log(ERROR, "%s: Invalid lnn file size.  Size: %d",
		    __func__, size);
		goto out;
	}

	/* Add one empty byte for the null character */
	buffer = calloc(1, size + 1);
	ASSERT(buffer);
	rewind(file);
	res = fread(buffer, 1, size, file);
	if (res != size) {
		log(ERROR, "%s: Failed to read lnn file. (%d)", __func__,
		    errno);
		goto out;
	}

	if (size > 10) {
		log(ERROR, "%s: Contents of lnn file too large.  Size: %d "
		    "Contents: %s", __func__, size, buffer);
		goto out;
	}

	*to_return = atoi(buffer);
	ASSERT(*to_return > 0, "Buffer: %s", buffer);
	ret = 0;

out:
	if (file)
		fclose(file);
	if (buffer) {
		free(buffer);
		buffer = NULL;
	}
	return ret;
}

int
read_lnn_file(char *path, int *to_return)
{
	int ret = -1, lock_fd = -1;

	lock_fd = lock_file(path, O_SHLOCK);
	if (lock_fd > -1) {
		ret = read_int_from_file(path, to_return);
		close(lock_fd);
	}

	return ret;
}

int
read_total_workers(int *total_workers)
{
	int ret = -1, lock_fd = -1;

	lock_fd = lock_file(SIQ_WORKER_POOL_TOTAL, O_SHLOCK); 
	if (lock_fd > -1) {
		ret = read_int_from_file(SIQ_WORKER_POOL_TOTAL, total_workers);
		close(lock_fd);
	}

	return ret;
}

static int
write_int_to_file(char *path, int value)
{	
	FILE *file = NULL;
	int res = 0;
	int ret = 0;
	int count = 0;
	char *to_write = NULL;
	
	asprintf(&to_write, "%d", value);
	ASSERT(to_write);

	file = fopen(path, "w");
	if (!file) {
		log(ERROR, "%s: failed to fopen %s", __func__, path);
		ret = -1;
		goto out;
	}

	count = strlen(to_write);
	res = fwrite(to_write, 1, count, file);
	if (res != count) {
		log(ERROR, "%s: failed to fwrite to %s %s", __func__, path, 
		    strerror(errno));
		ret = -1;
		goto out;
	}

out:
	if (file) {
		fclose(file);
	}
	if (to_write) {
		free(to_write);
		to_write = NULL;
	}
	return ret;
}

int
write_lnn_file(char *path, int max_workers)
{
	int ret = -1, lock_fd = -1;

	lock_fd = lock_file(path, O_EXLOCK);
	if (lock_fd > -1) {
		ret = write_int_to_file(path, max_workers);
		close(lock_fd);
	}

	return ret;
}

/* Record how many total workers are available on the cluster */
int
write_total_workers(int total_workers)
{
	int ret = -1, lock_fd = -1;

	/* Must be greater than 0 and less than 10 digits long. This enforces
	 * the same error conditions as read_total_workers. */
	ASSERT(total_workers > 0 && total_workers < 1000000000,
	    "total_workers == %d", total_workers);
	lock_fd = lock_file(SIQ_WORKER_POOL_TOTAL, O_EXLOCK); 
	if (lock_fd > -1) {
		ret = write_int_to_file(SIQ_WORKER_POOL_TOTAL, total_workers);
		close(lock_fd);
	}

	return ret;
}

/*
 * fork_accept_from
 *
 * Return a socket which has been accepted to a child, and continue listening.
 */
int
fork_accept_from(unsigned short port, unsigned *prot_ver, int debug,
    enum handshake_policy policy, int max_num_children, int lnn)
{
	int s, ret, status, max_children, flags;
	struct sockaddr_storage ss;
	socklen_t len;
	fd_set set;
	struct timeval timeout = {5, 0};
	bool exceeded_count = false;
	const int CHILD_EXCESS_WAIT_SECS = 5;
	struct version_status ver_stat, tmp_ver_stat;
	char *path = NULL;

	asprintf(&path, "%s/%d", SIQ_WORKER_POOL_DIR, lnn);
	ASSERT(path);

	max_children = max_num_children;
	if (lnn > 0) {
		//Read the path now
		ret = read_lnn_file(path, &max_children);
		if (ret < 0) {
			log(FATAL, "%s: failed to read_lnn_file", __func__);
		}
		log(DEBUG, "Initial max_children: %d", max_children);

		//Set up kevent for above path
		ret = add_lnn_kevent(path);
		if (ret != 0) {
			log(FATAL, "%s: failed to add_lnn_event", __func__);
		}
	}

	/* Make sure we clean up children. */
	if (signal(SIGCHLD, sigchld) == SIG_ERR)
		log(FATAL, "signal(SIGCHLD)");

	/* Set nonblocking to prevent select/accept race. */
	if ((s = listen_on(port, false)) < 0)
		log(FATAL, "Could not listen on port %d, may be in use", port);

	get_version_status(false, &ver_stat);

	for (;;) {
		while (waitpid(-1, &status, WNOHANG) > 0) {
			child_count--;
			log(TRACE, "%s: wait return. child count=%d", __func__,
			child_count);
		}

		FD_ZERO(&set);
		FD_SET(s, &set);
		if (lnn_kev_fd > 0) {
			FD_SET(lnn_kev_fd, &set);
		}

		/* select and accept are decoupled so that we can periodically 
		 * reap children even if new connections aren't being 
		 * established. once select returns with an actual new 
		 * connection on the listen socket, we can then accept 
		 * without blocking. */
		ret = select(MAX(s, lnn_kev_fd) + 1, &set, NULL, NULL,
		    &timeout);
		if (ret == -1 && errno != EINTR)
			break;

		if (ret <= 0 || (!FD_ISSET(s, &set) &&
		    (lnn_kev_fd <= 0 || (lnn_kev_fd > 0 &&
		    !FD_ISSET(lnn_kev_fd, &set))))) {
			continue;
		}

		//If the event is the worker pool lnn file, update limits
		if (lnn_kev_fd > 0 && FD_ISSET(lnn_kev_fd, &set)) {
			//Remove kevent
			remove_lnn_kevent();
			//Read file
			ret = read_lnn_file(path, &max_children);
			if (ret < 0) {
				log(FATAL, "%s: failed to read_lnn_file",
				    __func__);
			}
			log(DEBUG, "max_children updated to %d", max_children);
			//Readd kevent
			ret = add_lnn_kevent(path);
			if (ret != 0) {
				log(FATAL, "%s: failed to add_lnn_kevent",
				    __func__);
			}
			continue;
		}

		len = sizeof(struct sockaddr);
		ret = accept(s, (struct sockaddr *)&ss, &len);
		if (ret < 0) {
			//Maybe the socket went away due to timeout
			continue;
		}
		log(DEBUG, "Accepted connection %d", ret);

		/* Use the most recently-committed cluster version for the
		 * handshake. */
		if (ver_stat.local_node_upgraded) {
			get_version_status(false, &tmp_ver_stat);
			if (!tmp_ver_stat.local_node_upgraded) {
				/* A commit does not necessarily imply that
				 * the SIQ version changed, hence "<=". */
				ASSERT(ver_stat.committed_version <=
				    tmp_ver_stat.committed_version);
				log(NOTICE, "Upgrade committed (old version: "
				    "0x%x, new version: 0x%x)",
				    ver_stat.committed_version,
				    tmp_ver_stat.committed_version);
				memcpy(&ver_stat, &tmp_ver_stat,
				    sizeof(ver_stat));
			}
		}

		if (msg_handshake(ret, ver_stat.committed_version, prot_ver,
		    policy)) {
			close(ret);
			continue;
		}
		
		//If we have too many children, wait
		if (max_children > 0 && child_count >= max_children) {
			if (!exceeded_count) {
				log(DEBUG, "Max child count of %d reached. No "
				    "connections until other children exit.",
				    max_children);
				exceeded_count = true;
			}
			//Send general signal to exit if this is the pworker
			//Otherwise, this is the sworker and delay instead
			if (lnn > 0) {
				general_signal_send(ret, SIQ_GENSIG_BUSY_EXIT);
			} else {
				siq_nanosleep(CHILD_EXCESS_WAIT_SECS, 0);
			}
			close(ret);/////children太多了,需要关掉这个fd
			
			continue;
		}
		
		if (exceeded_count) {
			log(DEBUG, "Child count now below limit of of %d",
			    max_children);
			exceeded_count = false;
		}

		/* we can impose limits on the number of forked children by
		 * monitoring this count. we decrement the count when exited
		 * children are reaped. */
		child_count++;
		
		log(DEBUG, "child_count = %d max = %d", child_count, 
		    max_children);

		if (debug || !fork()) {
			/* We need to know what interface was used for the
			 * connection, as a message is returned to this client
			 * with a list of IPs for all nodes. E.g., A source
			 * cluster has a route to External-2 of the target
			 * cluster, but no route to External-1 of the
			 * target. When the target sworker sends a list of IPs
			 * for the cluster back to the source pworker, all IPs
			 * must be in the same interface as the initial
			 * connection to the sworker. */

			//Set back to blocking
			flags = fcntl(s, F_GETFL, 0);
			if (flags == -1)
				log(FATAL, "Failed to fcntl(F_GETFL): %s",
				    strerror(errno));
			if (fcntl(s, F_SETFL, flags & ~O_NONBLOCK) == -1)
				log(FATAL, "Failed to fcntl(F_SETFL): %s",
				    strerror(errno));

			len = sizeof(struct sockaddr_storage);

			if (getsockname(ret, (struct sockaddr *)&ss, &len)
			    == -1) {
				log(FATAL, "Failed to getsockname: %s",
				    strerror(errno));
			}
			inx_addr_from_sa_x((struct sockaddr *)&ss, &g_dst_addr);
			v4mapped_to_v4(&g_dst_addr);

			if (getpeername(ret, (struct sockaddr *)&ss, &len)
			    == -1) {
				log(FATAL, "Failed to getpeername: %s",
				    strerror(errno));
			}
			inx_addr_from_sa_x((struct sockaddr *)&ss, &g_src_addr);
			v4mapped_to_v4(&g_src_addr);

			log(DEBUG, "Connection on interface IP %s",
			    inx_addr_to_str(&g_dst_addr));
			close(s);
			return ret;
		}
		close(ret); /* The parent doesn't need this fd. */
	}

	return -1;
}

#ifdef DEBUG_PERFORMANCE_STATS

static int g_file_data_start;
static int g_file_data_len;
static struct timeval g_file_data_start_tv;

void
file_data_start()
{
	log(DEBUG, "%s", __func__);
	g_file_data_start = 1;
	g_file_data_len = 0;
	gettimeofday(&g_file_data_start_tv, NULL);
}

void
file_data_end()
{
	log(DEBUG, "%s", __func__);
	g_file_data_start = 0;
}

#endif // DEBUG_PERFORMANCE_STATS

static int
burst_send_data(burst_fd_t sender, char *buf, int len)
{
	int res;
	burst_pollfd_t pollfd;

	pollfd._fd = sender;
	pollfd._events = BURST_POLLOUT;
	pollfd._revents = 0;
	burst_poll(&pollfd, 1, -1);
	
	ASSERT(pollfd._revents & BURST_POLLOUT);
	log(TRACE, "Sending data via burst: %d bytes", len);
	res = burst_send(sender, buf, len);
	return res;
}

/*
 * full_send
 *
 * Send the whole buffer and don't return until you do, or have an error
 */
int
full_send(int fd, char *buf, int len)
{
	/* The threshold is to avoid too many and short (and likely inaccurate)
	 * sleeps. */
	const int THRESH    =  100000; /*   100,000 usec = 0.1 sec */
	const int MAXCREDIT = 5000000; /* 5,000,000 usec = 5 sec  */
	static struct timeval tv, lasttv;
	int sent = 0, got = 0, maxsend;
	int64_t diff;
	enum socket_type sock_type;
	burst_fd_t sender = 0;

	sock_type = sock_type_from_fd_info(fd);
	if (sock_type == SOCK_BURST) {
		sender = burst_fd_from_fd_info(fd);
		ASSERT(sender >= 0);
	}

	gettimeofday(&tv, 0);
	diff = tv_usec_diff(&tv, &lasttv);

	/*
	 * Break up each chunk sent to the maximum number of bytes that can
	 * be sent in 0.1 seconds according to the bandwidth limits:

| bandwidth kbits |   | 1000 bits |    |  1 Byte  |                   Bytes
| --------------- | * | --------- |  * | -------  | = bandwidth * 125 -----
|       sec       |   |   1 kbits |    |  8 bits  |                    sec

          And divide by 10 to get the number of bytes that can be sent in 1/10
          second.

  bandwidth * 125 Bytes/sec * 0.1 sec = bandwidth * 12.5 Bytes = (bandwidth * 25) / 2 Bytes

	* The choice of 0.1 second per chunk sent is arbitrary but reasonable to
	* regulate bandwidth.
	*
	* 1000 bits/kbit used instead of 1024 as that is how bandwidth is
	* measured.
	*/

	//Sanity check to make sure we don't int overflow the maxsend computation
	if (bandwidth > 85899345 || bandwidth <= 0) {
		maxsend = len;
	} else {
		maxsend = (bandwidth * 25) / 2;
	}

	while (sent < len) {
		errno = 0;
		if (sock_type == SOCK_TCP) {
			got = send(fd, buf + sent, MIN(len - sent, maxsend), 0);
		} else if (sock_type == SOCK_BURST) {
			got = burst_send_data(sender, buf + sent,
			    MIN(len - sent, maxsend));
		}

		/* Retry if:
		 * 1. No data was sent, 0 is a valid return value.
		 * 2. The socket is in non-blocking mode and we got EAGAIN.
		 *    This can happen if our send buffer is full. Since
		 *    full_send() only returns when the whole message is sent,
		 *    we need to keep retrying. If the pipe is broken, we will
		 *    get a different error than EAGAIN. */
		if (got == 0 || (got == -1 && errno == EAGAIN)) {
			siq_nanosleep(0, 1000000);
			continue;
		}

		if (got == -1)
			return -1;

		add_send_netstat(fd, got);
		sent += got;

		if (tracked_fd == fd && bandwidth > 0) {
			static int uleft;

        /*
                      | 8 bits |   | 1 sec           |   | 1 kbits    |   | 1,000,000 usecs |      X * 8000
            | X bytes | * | ------ | * | --------------  | * | ---------- | * | --------------- | =    ----------  usecs
                      |  byte  |   | bandwidth kbits |   | 1000 bytes |   | 1 second        |      bandwidth

	        Casting to int64_t is required to avoid overflow that prevents
	        any attempt to limit bandwidth for values near and above 32 mb/sec
        */
			uleft += (int64_t)got * 8000 / bandwidth -
			    (diff * got / len);

			/*
			 * Explanation for use of MAXCREDIT: Optimally, for a
			 * given bandwidth, data transfer will always be 0
			 * usec behind the time it expected to transfer all
			 * the data so far. If uleft is positive, it means
			 * that the data is being sent too fast, so a sleep is
			 * needed. If uleft is negative, it means that the
			 * data is being sent too slowly (or that there's big
			 * enough gaps in sending data that overall we're
			 * under the bandwidth). But if uleft is too far
			 * negative, there would have to be a big burst of
			 * data to catch up to the optimal bandwidth, and that
			 * big burst of data would exceed the desired
			 * bandwidth during the time it's being sent. By
			 * letting uleft not get MAXCREDIT usecs (5 seconds)
			 * behind, ensures that any catch up burst is very
			 * short.
			 */
			if (uleft < -MAXCREDIT)
				uleft = -MAXCREDIT;

			/* Don't sleep for less than the threshold */
			if (uleft > THRESH) {
				siq_nanosleep(0, uleft * 1000);
				/* We're caught up to the desired bandwidth */
				uleft = 0;
			}
		}
	}
	gettimeofday(&lasttv, 0);

#ifdef DEBUG_PERFORMANCE_STATS
	uint64_t total_usecs = tv_usec_diff(&lasttv, &tv);
	float bits_per_sec = (float) len / total_usecs * USECS_PER_SEC * 8;
	log(DEBUG, "%s: this msg %d bytes in %llu usecs. %.2f kbits/s",
	    __func__, len, total_usecs, bits_per_sec/1000);
	if (g_file_data_start) {
		g_file_data_len += len;
		total_usecs = tv_usec_diff(&lasttv, &g_file_data_start_tv);
		bits_per_sec = (float) g_file_data_len / total_usecs *
		    USECS_PER_SEC * 8;
		log(DEBUG, "%s: all data %d bytes in %llu usecs. %.2f kbits/s",
		    __func__, g_file_data_len, total_usecs, bits_per_sec/1000);
	}
#endif
	return 0;
}

int
get_bandwidth()
{
	return bandwidth;
}

void
set_bandwidth(int kbs)
{
	bandwidth = kbs;
}

static struct event_listener *ipc_listener = NULL;

int
ipc_connect(int *devid)
{
	struct isi_error *error = NULL;

	if (!ipc_listener)
		ipc_listener = gmp_create_listener(&error);

	if (!error && devid) {
		int devid_val;
		size_t val_len = sizeof(devid_val);
		if (sysctlbyname("efs.rbm.array_id", &devid_val,
		    &val_len, NULL, 0) != 0) {
			error = isi_system_error_new(errno,
			    "Failed to read device id");
		} else
			*devid = devid_val;
	}

	if (error) {
		log(ERROR, "Failed to create group change listener: %s",
		    isi_error_get_message(error));
		isi_error_free(error);
		if (ipc_listener) {
			event_listener_destroy(ipc_listener);
			ipc_listener = NULL;
		}
	}

	return ipc_listener ? event_listener_fileno(ipc_listener) : -1;
}

int
ipc_readgroup(int ipc_sock)
{
	struct isi_error *error = NULL;

	ASSERT(ipc_sock == (ipc_listener ?
	    event_listener_fileno(ipc_listener) : -1));

	gmp_event_recv(ipc_listener, &error);
	if (error) {
		log(FATAL, "Failed to receive group change event: %s",
		    isi_error_get_message(error));
		isi_error_free(error);
		return -1;
	} else
		return 0;
}
