#include <isi_util/checker.h>
#include <isi_util/check_helper.h>
#include <isi_util_cpp/check_helper.hpp>
#include <ifs/ifs_syscalls.h>

#include "cpool_message.h"

#define TEST_FILE_NAME "/tmp/cpool_envelope_testfile"

TEST_FIXTURE(setup_test)
{
	unlink(TEST_FILE_NAME);
}

TEST_FIXTURE(teardown_test)
{
	unlink(TEST_FILE_NAME);
}

SUITE_DEFINE_FOR_FILE(check_cpool_message,
	.mem_check = CK_MEM_LEAKS,
	.test_setup = setup_test,
	.test_teardown = teardown_test,
	.timeout = 0,
	.fixture_timeout = 1200);

class cpool_test_envelope : public cpool_envelope
{
protected:
	const char *get_outgoing_file_name() const { return TEST_FILE_NAME; };
	const char *get_incoming_file_name() const { return TEST_FILE_NAME; };
};

TEST(send_notifications)
{
	struct isi_error *error = NULL;
	cpool_test_envelope out_envelope;
	cpool_test_envelope in_envelope;

	/* Check for messages - should not be any */
	in_envelope.check(&error);
	fail_if(error != NULL, "Error checking messages: %#{}",
	    isi_error_fmt(error));

	fail_if(in_envelope.has_messages(),
	    "in_envelope should not have any messages present");

	/* Create an envelope and populate it with a couple of messages */
	out_envelope.add_message(CPM_RECIPIENT_RESTORE_COI, CPM_ACTION_NONE);
	out_envelope.add_message(CPM_RECIPIENT_NONE, CPM_ACTION_NOTIFY);

	out_envelope.send(&error);
	fail_if(error != NULL, "Error sending message: %#{}",
	    isi_error_fmt(error));

	/* Check whether we can retrieve the messages */
	in_envelope.check(&error);
	fail_if(error != NULL, "Error checking messages: %#{}",
	    isi_error_fmt(error));

	fail_unless(in_envelope.has_messages(),
	    "in_envelope should have messages present");

	const std::vector<cpool_message*> &messages =
	    in_envelope.get_messages();
	fail_if(messages.size() != 2,
	    "Incorrect number of messages found.  Expected 2, got %d",
	    messages.size());

	for (std::vector<cpool_message*>::const_iterator it = messages.begin();
	    it != messages.end(); ++it) {

		const cpool_message *message = *it;

		/*
		 * If current message matches one of the sent messages, we're
		 * ok
		 */
		if (message->get_recipient() == CPM_RECIPIENT_RESTORE_COI &&
		    message->get_action() == CPM_ACTION_NONE)
			continue;
		if (message->get_recipient() == CPM_RECIPIENT_NONE &&
		    message->get_action() == CPM_ACTION_NOTIFY)
			continue;

		fail_if(true,
		    "Unexpected message found.  Recipient: %d, action: %d",
		    message->get_recipient(), message->get_action());
	}
}
