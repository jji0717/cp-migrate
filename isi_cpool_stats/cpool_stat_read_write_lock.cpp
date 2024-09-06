#include "cpool_stat_read_write_lock.h"

#define MAX_CP_STAT_NAME_LENGTH 4096
#define CP_STAT_KEY_PATH "/tmp/cpool_stat_"
#define CP_STAT_FTOK_PROJ_ID 8675309

#define ERROR_SEMAPHORE_GET_FAILED "Failed to get semaphore"
#define ERROR_SEMAPHORE_INIT_FAILED "Failed to initialize semaphore"
#define ERROR_SEMAPHORE_OP_FAILED "Failed to execute semaphore operation"
#define ERROR_CP_STAT_NAME_TOO_LONG \
    "Key file name exceeds buffer size: %s"
#define ERROR_OPEN_FILE "Unable to open key file: %s"
#define ERROR_MULTIPLE_LOCK_CALLS "Cannot take same lock more than once"
key_t
cpool_stat_name_to_key(const char *name, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	key_t key = -1;
	char full_path_buff[MAX_CP_STAT_NAME_LENGTH + strlen(CP_STAT_KEY_PATH)];
	FILE *fd = NULL;

	if (strlen(name) >= MAX_CP_STAT_NAME_LENGTH) {
		error = isi_system_error_new(ENOSPC,
		    ERROR_CP_STAT_NAME_TOO_LONG, name);
		goto out;
	}

	/* Convert given name to a file name and make sure file exists */
	sprintf(full_path_buff, "%s%s", CP_STAT_KEY_PATH, name);

	fd = fopen(full_path_buff, "a+");
	if (fd == NULL) {
		error = isi_system_error_new(errno, ERROR_OPEN_FILE,
		    full_path_buff);
		goto out;
	}
	fclose(fd);

	/* Map the file and project id back to a unique key */
	key = ftok(full_path_buff, CP_STAT_FTOK_PROJ_ID);

out:
	isi_error_handle(error, error_out);
	return key;
}

cpool_stat_read_write_lock::~cpool_stat_read_write_lock()
{
	unlock(isi_error_suppress(IL_NOTICE));
	free(sem_name_);
}

static time_t
sem_get_sem_otime(int sem_id, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	union semun arg;
	struct semid_ds seminfo;
	time_t sem_otime = 0;

	arg.buf = &seminfo;
	if (semctl(sem_id, 0, IPC_STAT, arg) == -1)
		ON_SYSERR_GOTO(out, errno, error, ERROR_SEMAPHORE_INIT_FAILED);

	sem_otime = arg.buf->sem_otime;

out:
	isi_error_handle(error, error_out);

	return sem_otime;
}

/* See header for some details on the locking logic */
void
cpool_stat_read_write_lock::initialize_semaphores(struct isi_error **error_out)
{
	/* Don't initialize more than once */
	if (sem_id_ != -1)
		return;

	ASSERT(sem_name_ != NULL && strlen(sem_name_) > 0);

	struct isi_error *error = NULL;
	int sem_id = -1;
	time_t sem_otime = 0;
	const int num_semaphores = 2;//两个信号量,第一个是读,第二个是写
	const int MAX_TRIES = 10;
	const int sem_permissions = 0666;

	/* Exclusively create semaphore and give read/write permissions */
	int sem_flags = IPC_CREAT | IPC_EXCL | sem_permissions;

	key_t semaphore_key = cpool_stat_name_to_key(sem_name_, &error);  ////将给定字符串转为semaphore key
	ON_ISI_ERROR_GOTO(out, error);

	sem_id = semget( semaphore_key, num_semaphores, sem_flags );///创建一个信号量集,或者访问一个存在的信号量集

	/*
	 * Failed to create semaphore, check for EEXIST, to see if we can try
	 * without IPC_CREAT/IPC_EXCL to get existing semaphore
	 */
	if (sem_id == -1) {
		int i = 0;

		if (errno != EEXIST)
			ON_SYSERR_GOTO(out, errno, error, ERROR_SEMAPHORE_GET_FAILED);

		/* EEXIST */
		sem_flags = sem_permissions;
		sem_id = semget( semaphore_key, num_semaphores, sem_flags);
		if (sem_id == -1)
			ON_SYSERR_GOTO(out, errno, error, ERROR_SEMAPHORE_GET_FAILED);

		/* Wait for the semaphores to get initialized by the
		 * thread/process that created them
		 */
		for (i = 0; i < MAX_TRIES; ++i) {
			sem_otime = sem_get_sem_otime(sem_id, &error);
			ON_ISI_ERROR_GOTO(out, error);

			if (sem_otime != 0)
				break;
			sleep(1);
		}
		if (i == MAX_TRIES)
			ON_SYSERR_GOTO(out, EWOULDBLOCK, error,
			    ERROR_SEMAPHORE_INIT_FAILED);
	/* Created a new semaphore, do initialization */
	} else {
		/* Initialize to 0 which means ready to use */
		unsigned short sarray[num_semaphores] = { 0 };//初始时,s_r=0 s_w=0 表示当前读的人为0,写的人为0
		union semun arguments;
		struct sembuf op;

		arguments.array = sarray;
		if (semctl(sem_id, 0, SETALL, arguments) == -1)
			ON_SYSERR_GOTO(out, errno, error, ERROR_SEMAPHORE_INIT_FAILED);

		/* Dummy semop to update sem_otime and finish the
		 * initialization, preventing any init races */
		op.sem_num = 0;
		op.sem_op = 0;
		op.sem_flg = 0;
		if (semop(sem_id, &op, 1) == -1)
			ON_SYSERR_GOTO(out, errno, error, ERROR_SEMAPHORE_INIT_FAILED);

		/* Verify sem_otime is not zero */
		sem_otime = sem_get_sem_otime(sem_id, &error);
		ON_ISI_ERROR_GOTO(out, error);

		ASSERT(sem_otime != 0);
	}
	sem_id_ = sem_id;
out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_read_write_lock::do_lock(bool wait_for_read, bool wait_for_write,
    short read_delta, short write_delta, struct isi_error **error_out) const
{
	ASSERT(sem_id_ != -1);

	const int max_operations = 4;
	const int read_op_index = 0;
	const int write_op_index = 1;

	struct isi_error *error = NULL;
	struct sembuf ops[max_operations];
	int semop_result = 0;
	int op_count = 0;
	//读信号量:0 写信号量:1  
	if (wait_for_read) {
		ops[op_count].sem_num = read_op_index; //0 第0个信号量:即读信号量
		ops[op_count].sem_op = 0;              //期望读为0  wait for s_w == 0
		ops[op_count].sem_flg = 0;
		op_count++;
	}

	if (wait_for_write) { 
		ops[op_count].sem_num = write_op_index; //1 第1个信号量: 即写信号量
		ops[op_count].sem_op = 0;              ////期望写为0  wait_for s_w == 0
		ops[op_count].sem_flg = 0;
		op_count++;
	}

	if (read_delta != 0) {
		ops[op_count].sem_num = read_op_index;   /////设置读信号量
		ops[op_count].sem_op = read_delta;       ///读信号量数目增加read_delta  increment(+1)/decrement(-1) s_r
		ops[op_count].sem_flg = SEM_UNDO;
		op_count++;
	}

	if (write_delta != 0) {
		ops[op_count].sem_num = write_op_index;  ////设置写信号量
		ops[op_count].sem_op = write_delta;      //写信号量数目增加write_delta  increment(+1)/decrement(-1) s_w
		ops[op_count].sem_flg = SEM_UNDO;
		op_count++;
	}

	while (true) {
		semop_result = semop(sem_id_, ops, op_count/*信号量操作的个数*/);////成功返回0,失败返回-1
		if (!semop_result)
			break;
		printf("%s called semop_result:%d\n", __func__, semop_result);
		if (errno == EINTR)
		{
			printf("%s called errno == EINTR\n", __func__);
		}
		if (errno != EINTR)
			ON_SYSERR_GOTO(out, errno, error, ERROR_SEMAPHORE_OP_FAILED);
	}
out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_read_write_lock::unlock(struct isi_error **error_out)
{
	if (is_read_lock_)
		unlock_for_read(error_out);
	else
		unlock_for_write(error_out);
}

void
cpool_stat_read_write_lock::lock_for_read(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (lock_taken_) {
		error = isi_system_error_new(EEXIST, ERROR_MULTIPLE_LOCK_CALLS);
		goto out;
	}

	initialize_semaphores(&error);
	ON_ISI_ERROR_GOTO(out, error);

	do_lock(false/*拿锁前,不在乎读者数量*/, true/*拿锁前,希望写着数量为0*/, 1/*拿到锁后,读者数量+1*/, 0/*拿到锁后,写者数量不变*/, &error);
	ON_ISI_ERROR_GOTO(out, error);

	is_read_lock_ = true;
	lock_taken_ = true;
out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_read_write_lock::lock_for_write(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (lock_taken_) {
		error = isi_system_error_new(EEXIST, ERROR_MULTIPLE_LOCK_CALLS);
		goto out;
	}

	initialize_semaphores(&error);
	ON_ISI_ERROR_GOTO(out, error);

	do_lock(true/*拿到锁前,希望读者数为0*/, true/*拿到锁前,希望写者数目为0*/, 1/*拿到锁后,已有读者数+1*/, 1/*拿到锁后,已有写者数+1*/, error_out);
	ON_ISI_ERROR_GOTO(out, error);

	is_read_lock_ = false;
	lock_taken_ = true;
out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_read_write_lock::unlock_for_read(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (lock_taken_) {
		/* Lock cannot have been taken unless sems were initialized */
		ASSERT(sem_id_ != -1);

		do_lock(false, false, -1/*解锁后,已有读者数-1*/, 0, error_out);
		ON_ISI_ERROR_GOTO(out, error);
	}

	lock_taken_ = false;
out:
	isi_error_handle(error, error_out);
}

void
cpool_stat_read_write_lock::unlock_for_write(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (lock_taken_) {
		/* Lock cannot have been taken unless sems were initialized */
		ASSERT(sem_id_ != -1);

		do_lock(false, false, -1/*解锁后,已有读者数-1*/, -1/*解锁后,已有写者数-1*/, error_out);
		ON_ISI_ERROR_GOTO(out, error);
	}

	lock_taken_ = false;
out:
	isi_error_handle(error, error_out);
}
