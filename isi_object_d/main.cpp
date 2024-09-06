#include "fcgi_config.h"

#include <stdlib.h>
#include <unistd.h>
#include <pwd.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/event.h>
#include <sys/isi_ntoken.h>
#include <sys/time.h>

#include <isi_daemon/isi_daemon.h>
#include <isi_object_api/oapi.h>
#include <isi_object_api/oapi_gcfg.h>
#include <isi_ilog/ilog.h>
#include <fcgiapp.h>
#include <isi_gmp/isi_gmp.h>

#include <isi_rest_server/fcgi_helpers.h>
#include <isi_rest_server/request_thread.h>
#include <isi_object_api/oapi_mapper.h>
#include <isi_util_cpp/safe_strerror.h>

//cause jemalloc to abort on out of memory
const char * malloc_conf = "xmalloc:true";

namespace {
// we should make the following configurable.
const int UPPER_MAX_THREAD = 1000;
const int LOWER_MAX_THREAD = 1;

const int LOWER_MAX_REQUEST = 10;

const int LOWER_MAX_BACKLOG = 1;

// default limit numbers:
const int max_threads = 50;
const int max_waiting = 100;

const int listen_backlog = 100;

gci_base      *cfg_bas = NULL;
oapi_cfg_root *cfg_cfg = NULL;

// The following is the same value as the FastCgiExternalServer -socket option
static void
print_usage()
{
	printf("Usage:\n");
	printf("isi_object_d [options]\n\n");
	printf("Following options are supported:\n");
	printf("-h	Show this message.\n");
	printf("-v	Turn on verbose log, default[non-verbose].\n");
	printf("-s	Run the program in non-daemon mode.\n");
	printf("-t	Worker thread counts, default[%d], valid range[%d-%d].\n",
	    max_threads, LOWER_MAX_THREAD, UPPER_MAX_THREAD);
	printf("-w	Pending request limit, default[%d], valid range[%d-].\n",
	    max_waiting, LOWER_MAX_REQUEST);
	printf("-b	Listen backlog limit, default[%d], valid range[%d-].\n",
	    listen_backlog, LOWER_MAX_BACKLOG);
}

inline void
init_log(const char *app_name, const char *log_file, bool verbose)
{
	struct ilog_app_init init =
	{
		.full_app_name = app_name,
		.component = "",
		.job = "",
		.syslog_program = (char *)app_name,
		.default_level =
		    (verbose ? IL_DEBUG_PLUS : IL_INFO_PLUS) | IL_DETAILS,
		.use_syslog = true,
		.use_stderr = false,
		.log_thread_id = true,
		.syslog_facility = LOG_DAEMON,
		.log_file = log_file,
		.syslog_threshold =
		    (verbose ? IL_DEBUG_PLUS : IL_INFO_PLUS) | IL_DETAILS,
	};

	ilog_init(&init, true, true);
}


unsigned received_signals = 0;

void
on_signal(int sig)
{
	switch (sig) {
	case SIGHUP:
	case SIGUSR1:
	case SIGUSR2:
	case SIGTERM:
		received_signals |= (1 << sig);
		break;

	default:
		ilog(IL_TRACE, "Got unhandled signal %d", sig);
	}
}

void
monitor_fd(kevent_set &events, int fd)
{
	struct kevent kev;
	EV_SET(&kev, fd, EVFILT_READ, EV_ADD | EV_ONESHOT, 0, 0, 0);
	kevent_set_add(&events, kev);
}

void
receive_fcgi_request(int sock, request_thread_pool &pool)
{
	fcgi_request *req = new fcgi_request(sock, FCGI_RECV_CRED_FD);////FCGX_InitRequest(FCGX_Request, sock, flags)

	if (FCGX_Accept_r(req) < 0)  //////FCGX_Accept_r(req)
		_exit(EXIT_FAILURE);

	ILOG_START(IL_TRACE) {
		struct native_token *token = NULL;

		if ((token = fd_to_ntoken(req->credFd)) == NULL)
			fmt_print(ilogfmt, "Error converting cred_fd to "
			    "token: %s", safe_strerror().c_str());
		else {
			fmt_print(ilogfmt, "%{}", native_token_fmt(token));
			ntoken_free(token);
		}
	} ILOG_END;

	if (!pool.put_request(req)) {
		response output(req->get_output_stream());
		output.set_status(ASC_SERVICE_UNAVAILABLE);
		output.send();
		ilog(IL_ERR, "Error: Failed to put rq:%p to queue",
		    req);
		delete req;
	}
}


oapi_cfg_root *
reload_config()
{
	oapi_cfg_root *new_cfg = NULL;
	isi_error     *error   = NULL;

	if (cfg_bas == NULL) {
		cfg_bas = gcfg_open(GCI_OAPI_CONFIG_TREE, 0, &error);
		if (error)
			goto out;
	}

	gci_read_path(cfg_bas, "", &new_cfg, &error);
	if (error)
		goto out;

	std::swap(cfg_cfg, new_cfg);

out:
	ASSERT(cfg_cfg, "Unable to load initial config %{}",
	    isi_error_fmt(error));

	if (new_cfg)
		gci_free_path(cfg_bas, "", new_cfg, NULL);

	if (error)
		isi_error_free(error);

	return cfg_cfg;
}

void
close_config()
{
	if (cfg_bas) {
		if (cfg_cfg)
			gci_free_path(cfg_bas, "", cfg_cfg, NULL);
		gcfg_close(cfg_bas);
	}

	cfg_bas = NULL;
	cfg_cfg = NULL;
}

} // anonymous namespace

int
main(int argc, char *argv[])
{
	bool verbose = false, show_help = false, standalone = false;
	int opt = -1;
	struct isi_error *error = NULL;
	struct sigaction sigact = { };
	int kq;
	struct kevent_set kevs = KEVENT_SET_INITIALIZER;
	int server_sock;

	oapi_cfg_root *cfg = reload_config();

	int in_th_cnt = cfg->pool->max_threads; //isi_object_api/oapi.gcfg 50
	int in_rq_cnt = cfg->pool->max_waiting; /////100
	int in_bl_cnt = cfg->listen_backlog;
	const char *socket_path = cfg->socket_path;
	const char *socket_owner = cfg->socket_owner;

	while ((opt = getopt(argc, argv, "vshw:t:")) != -1) {
		switch (opt) {
			case 'v' :
				verbose = true;
				break;
			case 's' :
				standalone = true;
				break;
			case 't' :
				in_th_cnt = strtol(optarg, NULL, 10);
				if (in_th_cnt < LOWER_MAX_THREAD || 
				    in_th_cnt > UPPER_MAX_THREAD)
					show_help = true;
				break;
			case 'w' :
				in_rq_cnt = strtol(optarg, NULL, 10);
				if (in_rq_cnt < LOWER_MAX_REQUEST)
					show_help = true;
				break;
			case 'b' :
				in_bl_cnt = strtol(optarg, NULL, 10);
				if (in_bl_cnt < LOWER_MAX_BACKLOG)
					show_help = true;
				break;
			case 'h' :
			
			default :
				show_help = true;
				break;
		}
	}
	
	if (show_help) {
		print_usage();
		return 0;
	}

	cfg->listen_backlog = in_bl_cnt;
	cfg->pool->max_threads = in_th_cnt;
	cfg->pool->max_waiting = in_rq_cnt;
		
	// Initialize as a daemon process
	struct isi_error *ie = NULL;
	struct isi_daemon *dmn = NULL;
	const char *daemon_name = NULL;

	daemon_name = getprogname();

	init_log(daemon_name, "/var/log/isi_object_d.log", verbose);

	ilog(IL_INFO, "%s initializing", daemon_name);
	ilog(IL_INFO, "thread count:%d",
	    in_th_cnt);
	ilog(IL_INFO, "pending request limit:%d",
	    in_rq_cnt);
	ilog(IL_INFO, "listen backlog limit:%d",
	    in_bl_cnt);

	// Ensure we are in quorum and register signal handler to be self
	// terminated on SIGQUORUM
	gmp_wait_on_quorum(NULL, &error);
	if (error) {
		ilog(IL_ERR, "gmp_wait_on_quorum error: %s",
		    isi_error_get_message(error));
		isi_error_free(error);
		return EXIT_FAILURE;
	}

	if (!standalone) {
		dmn = isi_daemon_init(daemon_name, true, &ie);

		if (ie) {
			ilog(IL_ERR, "Error initializing %s: %s",
			    daemon_name,
			    isi_error_get_message(ie));
			isi_error_free(ie);
			return EXIT_FAILURE;
		}
	}

	sigact.sa_handler = on_signal;
	sigaction(SIGHUP,  &sigact, 0);
	sigaction(SIGUSR1, &sigact, 0);
	sigaction(SIGUSR2, &sigact, 0);
	sigaction(SIGTERM, &sigact, 0);

	/* Initialize fcgi */
	if (FCGX_Init() != 0) {
		ilog(IL_ERR, "failure initializing FCGX");
		return EXIT_FAILURE;
	}

	/* Create listener sockets */
	server_sock = FCGX_OpenSocket(socket_path, in_bl_cnt);
	if (server_sock < 0) {
		ilog(IL_ERR, "Error opening socket %s: %s",
		    socket_path, safe_strerror().c_str());
		return EXIT_FAILURE;
	}

	/* Allow mortals to connect to us */
	if (chmod(socket_path, 0666) < 0) {
		ilog(IL_ERR,
		    "Error granting world permissions to socket %s: %s",
		    socket_path, safe_strerror().c_str());
		return EXIT_FAILURE;
	}

	/* Change the owner to daemon so apache can write to the socket */
	struct passwd *pd = getpwnam(socket_owner);
	if (pd == NULL) {
		ilog(IL_ERR, "Error looking up socket owner");
		return EXIT_FAILURE;
	}

	if (chown(socket_path, pd->pw_uid, pd->pw_gid) < 0) {
		ilog(IL_ERR, "Error changing socket %s to owner %s: %s",
		    socket_owner, socket_path,
		    safe_strerror().c_str());
		return EXIT_FAILURE;
	}


	/* Create kernel queue */
	if ((kq = kqueue()) < 0) {
		ilog(IL_ERR, "kevent kqueue creation error: %s",
		    safe_strerror().c_str());
		return EXIT_FAILURE;
	}

	if (!init_oapi(*cfg)) {
		ilog(IL_ERR, "Error initializing object store");
		return EXIT_FAILURE;
	}

	/* Create request pool */
	request_thread_pool pool(oapi_mapper,
	    false, in_rq_cnt, in_th_cnt);

	/* Daemon loop */
	while (true) {
		int      nkevs;
		int      due_secs = 5;
		timespec poll_ts  = { };

		/* Register events */
		kevent_set_truncate(&kevs);
		monitor_fd(kevs, server_sock);
		nkevs = kevent_set_size(&kevs);
		if (kevent(kq, kevs.events, nkevs,
			NULL, 0, NULL) < 0) {
			ilog(IL_ERR, "kevent() unable to add events: %s",
			    safe_strerror().c_str());
			break;
		}

		/* Wait for events */
		if (received_signals)
			due_secs = 0;
		poll_ts.tv_sec = due_secs;
		nkevs = kevent(kq, NULL, 0, kevs.events, nkevs, &poll_ts);
		if (nkevs < 0 && errno != EINTR) {
			ilog(IL_ERR, "kevent() error in select: %s",
			    safe_strerror().c_str());
			break;
		}

		/*
		 * Process received signals.
		 */
		if (received_signals & (1 << SIGHUP)) {
			cfg = reload_config();
			notify_config_change(*cfg);
		}

		if (received_signals & (1 << SIGTERM))
			break;

		received_signals = 0;

		/* Handle other wakeups */
		for (int i = 0; i < nkevs; ++i) {
			int id = kevs.events[i].ident;

			if (id == server_sock) {
				receive_fcgi_request(server_sock, pool); ///这个server_sockk就是监听的socket fd
				continue;
			}

			ilog(IL_NOTICE, "kevent() unknown fd %d", id);
		}
	}

	/* clean up */

	pool.shutdown();

	close(server_sock);

	close_config();

	if (dmn)
		isi_daemon_destroy(&dmn);
}
