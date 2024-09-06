/**
 *  @file iobj_licensing.c
 *
 * A licensing module for the object library
 *
 * Copyright (c) 2011
 *	Isilon Systems, LLC.  All rights reserved.
 */
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>

#include <isi_util/isi_error.h>
#include <isi_preconfig_transport/pthread_wrappers.h>

#include "ostore_internal.h"
#include "iobj_licensing.h"
#include "isi_licensing/licensing.h"

pthread_once_t checker_control = PTHREAD_ONCE_INIT;

// licensed object library; defined as ISI_LICENSING_OBJ_* in
// isi_licensing/licensing.h
static const char *iobj_lic_module;
// check if object library license is valid
static bool iobj_lic_valid;

static pthread_cond_t checker_wakeup;
static pthread_cond_t checker_done;
static pthread_mutex_t lic_mutex = PTHREAD_MUTEX_INITIALIZER; 
static bool lic_check_requested;
static bool lic_check_bypass;

static void *
checker_thread(void *arg)
{
	enum isi_licensing_status lic_status;
	struct timespec ts;
	int res;

	pthread_mutex_lock_a(&lic_mutex);
	
	// poll for license validity
	while (1) {
		// wait to be woken up or timeout
		res = clock_gettime(CLOCK_MONOTONIC, &ts);
		ASSERT(res != -1);
		ts.tv_sec += IOBJ_LICENSE_CHECK_PERIOD;

		while (!lic_check_requested) {
			res = pthread_cond_timedwait(&checker_wakeup,
							&lic_mutex,
							&ts);
			if (res == ETIMEDOUT) {
				break;
			}
		} 
		
		// evaluate license validity
		if (iobj_lic_module==NULL)
			continue;
		lic_status = isi_licensing_module_status(iobj_lic_module);

		switch (lic_status) {
		case ISI_LICENSING_LICENSED:
			// log on transitions
			if (iobj_lic_valid == false) {
				iobj_lic_valid = true;
				// log transition; TODO replace with ilog
				//printf("\nObject library license is now valid\n");
			}
			break;
		case ISI_LICENSING_EXPIRED:
			if (iobj_lic_valid == true) {
				iobj_lic_valid = false;
				// log transition; TODO replace with ilog
				//printf("\nObject library license has expired\n");
			}
			break;
		default:
			if (iobj_lic_valid == true) {
				iobj_lic_valid = false;
				// log transition; TODO replace with ilog
				// ; TODO replace with ilog
				//printf("\nObject library license error %d\n", lic_status);
			}
			break;
		}

		// if this check was triggered for a request, send a response
		if (lic_check_requested) {
			pthread_cond_broadcast_a(&checker_done);
			lic_check_requested = false;
		}
	}

	pthread_mutex_unlock_a(&lic_mutex);
}

static void
checker_init(void)
{
	// initialize conds to use CLOCK_MONOTONIC
	ipt_cond_init(&checker_wakeup);
	ipt_cond_init(&checker_done);
	
	// start thread
	pthread_t thr;
	pthread_create_a(&thr, 0, checker_thread, NULL);
}

void
iobj_set_license_ID(const char *module_id)
{
	int res;

	ASSERT(module_id);

	// lock
	pthread_mutex_lock_a(&lic_mutex);
	
	if (strcmp(module_id, IOBJ_LICENSING_UNLICENSED) == 0) {
		lic_check_bypass = true;
		goto out;
	} else
		lic_check_bypass = false;

	// Only, once, start a thread that periodically checks
	// for license validity
	res = pthread_once(&checker_control, checker_init);
	ASSERT(res == 0);

	iobj_lic_module = module_id;
	
	// send request
	lic_check_requested = true;
	pthread_cond_signal_a(&checker_wakeup);

	// wait for response
	while (lic_check_requested)
		pthread_cond_wait_a(&checker_done, &lic_mutex);

 out:
	// unlock
	pthread_mutex_unlock_a(&lic_mutex);
 
}

bool
iobj_is_license_valid(void)
{
	bool lic_valid;

	pthread_mutex_lock_a(&lic_mutex);

	if (lic_check_bypass) { 
		lic_valid = true;
		goto out;
	}

	lic_valid = iobj_lic_valid;

 out:
	pthread_mutex_unlock_a(&lic_mutex);

	return lic_valid; 
}
