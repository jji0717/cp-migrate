#include "isi_migrate/migr/isirep.h"
#include "isi_migrate/config/siq_target.h"

int
main(int argc, char **argv)
{
	struct isi_error *error = NULL;
	int lock_fd;
	struct target_records *tdb;

	if (argc != 2) {
		fprintf(stderr, "Need only a policy name for an argument\n");
		exit(-1);
	}

	lock_fd = siq_target_acquire_lock(O_SHLOCK, &error);
	if (lock_fd == -1) {
		fprintf(stderr, "Can't get lock on TDB: %s\n", isi_error_get_message(error));
		exit(-1);
	}
	if (siq_target_status_read(&tdb, &error) == -1) {
		fprintf(stderr, "Can't read TDB: %s\n", isi_error_get_message(error));
		exit(-1);
	}

	struct target_record *record = tdb->record;
	while (record) {
		if (strcmp(record->policy_name, argv[1]) == 0)
			break;
		record = record->next;
	}

	if (!record)
		fprintf(stderr, "Can't find policy '%s'\n", argv[1]);
	
	else if (siq_target_cancel_request(record->id, &error) != CLIENT_CANCEL_SUCCESS)
		fprintf(stderr, "Cancel failed: %s\n", isi_error_get_message(error));
	else
		printf("Canceled\n");

}
