#include <stdio.h>
#include <sysexits.h>
#include <isi_migrate/migr/isirep.h>

static int g_sock = -1;
static struct timeval timeout = {2, 0};

static int
bandwidth_callback(struct generic_msg *m, void *ctx)
{
	printf("New ration = %d\n", m->body.bandwidth.ration);
	return 0;
}

static int
disconnect_callback(struct generic_msg *m, void *ctx)
{
	errx(EX_SOFTWARE, "Disconnected, exiting");
	return 0;
}

static int
timer_cb(void *ctx)
{
	struct generic_msg msg = {};
	char host[] = "jh-head-XX";
	int i;

	for (i = 1; i <= 12; i++) {
		msg.head.type = BANDWIDTH_STAT_MSG;
		sprintf(host, "jh-head-%d", i);
		msg.body.bandwidth_stat.name = host;
		msg.body.bandwidth_stat.bytes_sent = UINT32_MAX;
		msg_send(g_sock, &msg);
	}

	printf("Timeout. %u\n", UINT32_MAX);
	migr_register_timeout(&timeout, timer_cb, NULL);

	return 0;
}

int
main(int argc, char *argv[])
{
	struct isi_error *error = NULL;
	struct generic_msg msg = {};
	struct version_status ver_stat;
	struct ilog_app_init init = {
		.full_app_name = "siq_generator",
		.default_level = IL_FATAL,
		.use_syslog = true,
		.syslog_facility = LOG_DAEMON,
		.syslog_program = "isi_migrate",
		.log_file= "",
		.syslog_threshold = IL_TRACE_PLUS,
		.component = "generator",
		.job = "",
	};
	
	ilog_init(&init, false, true);
	set_siq_log_level(INFO);

	if (argc != 2)
		errx(EX_USAGE, "Need right arguments");

	get_version_status(true, &ver_stat);

	g_sock = connect_to(argv[1], 3148, POL_MATCH_OR_FAIL,
	    ver_stat.committed_version, NULL, NULL, false, &error);
	if (g_sock < 0)
		errx(EX_NOHOST, "failed to connect to %s:3148: %s", argv[1],
		    isi_error_get_message(error));

	migr_add_fd(g_sock, "dontaccept", "bw_host");
	migr_register_callback(g_sock, BANDWIDTH_MSG,
	    bandwidth_callback);
	migr_register_callback(g_sock, DISCON_MSG,
	    disconnect_callback);

	msg.head.type = BANDWIDTH_INIT_MSG;
	msg.body.bandwidth_init.policy_name = "!!!generator!!!";
	msg.body.bandwidth_init.num_workers = 1;
	msg.body.bandwidth_init.priority = 0;
	msg_send(g_sock, &msg);

	migr_register_timeout(&timeout, timer_cb, NULL);

	migr_process();

	return 0;
}
