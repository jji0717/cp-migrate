#include <sys/isi_ntoken.h>
#include <sys/ucred.h>

#include <errno.h>
#include <unistd.h>

#include <isi_ilog/ilog.h>
#include <isi_util/isi_assert.h>
#include <isi_util_cpp/scoped_clean.h>

#include "api_error.h"
#include "scoped_settcred.h"

scoped_settcred::scoped_settcred(int fd, bool run_as_user)
	: run_as_user_(run_as_user)
{
	if (run_as_user_)
		ilog(IL_DEBUG, "Assuming user cred %u", fd);

	if (run_as_user_ && settcred(fd, TCRED_FLAG_THREAD, NULL)) {
		throw api_exception(AEC_EXCEPTION,
		    "Error setting user credentials.");
	}
}

scoped_settcred::~scoped_settcred()
{
	if (run_as_user_)
		ilog(IL_DEBUG, "Reverting from user cred");

	if (run_as_user_)
		if (reverttcred())
			ASSERT(0, "failed to reverttcred: %d", errno);
}

/**
 * Must only be called inside the scope of a scoped_settcred(fd, true).
 */
scoped_settcred_root::scoped_settcred_root(int fd, bool run_as_root)
	: user_token_fd_(fd), run_as_root_(run_as_root)
{
	if (run_as_root_)
		ilog(IL_DEBUG, "Assuming root cred, and saving user cred %u",
		    user_token_fd_);

	if (run_as_root_)
		if (reverttcred())
			throw api_exception(AEC_EXCEPTION,
			    "Error reverting user credentials.");
}

scoped_settcred_root::~scoped_settcred_root()
{
	if (run_as_root_)
		ilog(IL_DEBUG, "Reverting from root to user cred %u",
		    user_token_fd_);

	if (run_as_root_ && settcred(user_token_fd_, TCRED_FLAG_THREAD, NULL))
		ASSERT(0, "failed to settcred: %d", errno);
}

/*
 * Scoped Set Zone
 */

/**
 * Change the zone of the current thread.
 *
 * PAPI handlers should validate whether a zone change is permitted
 * by calling zone_common::validate_cred_zone before calling this function.
 * For clarity, @a peer_fd can be set to CRED_NOT_USED if @a use_cred is false.
 *
 * @param peer_fd  The credential of user requesting the change.
 * @param use_cred True: change zid of peer_fd.
 *                 False: change zid of current thread
 * @param zid      Zone ID to which to change
 */
scoped_set_zone::scoped_set_zone(int peer_fd, bool use_cred, zid_t zid)
	: set_zid_(true)
{

	// Check for no-op case
	if (!use_cred && (zid_t)getzid() == zid) {
		set_zid_ = false;
		return;
	}

	if (set_zid_) {
		native_token *zone_tok = NULL;
		scoped_ptr_clean<native_token>
		    tok_clean(zone_tok, ntoken_free);

		zone_tok = ntoken_get();
		zone_tok->nt_fields |= NTOKEN_ZID;
		zone_tok->nt_zid = zid;

		// -1 means update the current cred
		int new_cred = -1;
		if (use_cred)
			new_cred = peer_fd;

		if (settcred(new_cred,
		    TCRED_FLAG_THREAD|TCRED_FLAG_TOKEN_UPDATE, zone_tok)) {
			struct isi_error *error = isi_system_error_new(errno,
			    "Resources are temporarily unavailable");
			ilog(IL_ERR, "Error setting zone %u", zid);
			api_exception::throw_on_error(error);
		}
	}
}

scoped_set_zone::~scoped_set_zone()
{
	if (set_zid_) {
		if (reverttcred())
			ASSERT(0, "Failed to reverttcred: %d", errno);
	}
}
