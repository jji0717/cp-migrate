#include <stdio.h>

#include <ifs/ifs_syscalls.h>
#include <isi_util/syscalls.h>
#include <isi_util/isi_error.h>
#include "cpool_message.h"

#define ENVELOPE_OUTGOING_FILE "/ifs/.ifsvar/modules/cloud/cpool_messages"
#define ENVELOPE_INCOMING_FILE "/tmp/cpool_messages"

void
cpool_message::append_to(FILE *f, struct isi_error **error_out) const
{
	ASSERT(f != NULL);
	struct isi_error *error = NULL;

	int ret = fprintf(f, "{%d,%d,%d}\n", version_, recipient_, action_);

	if (ret < 0) {
		error = isi_system_error_new(errno,
		    "Failed to append to file");
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
}

void
cpool_message::read_from(FILE *f, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int fields_populated = -1;
	int local_version = -1, local_recipient = -1, local_action = -1;

	fields_populated = fscanf(f, "{%d,%d,%d}\n", &local_version,
	    &local_recipient, &local_action);

	if (fields_populated == EOF) {
		error = isi_system_error_new(errno, "Unable to read message");
		goto out;
	}

	if (fields_populated != 3) {
		error = isi_system_error_new(EINVAL, "Invalid message format");
		goto out;
	}

	if (local_version != version_) {
		error = isi_system_error_new(EINVAL, "Unsupported version");
		goto out;
	}

	recipient_ = (cpool_message_recipient)local_recipient;
	action_ = (cpool_message_action)local_action;

 out:
	isi_error_handle(error, error_out);
}

cpool_envelope::~cpool_envelope()
{
	for (std::vector<cpool_message*>::iterator it = messages_.begin();
	    it != messages_.end(); ++it) {
		delete *it;
	}

	messages_.clear();
}

void
cpool_envelope::add_message(cpool_message_recipient to,
    cpool_message_action what)
{
	add_message(new cpool_message(to, what));
}

void
cpool_envelope::add_message(cpool_message *message)
{
	messages_.push_back(message);
}

void
cpool_envelope::send(struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	FILE *f = fopen(get_outgoing_file_name(), "w");
	int fpf_ret = 0;

	if (f == NULL) {
		error = isi_system_error_new(errno,
		    "Failed to open file \"%s\"", get_outgoing_file_name());
		goto out;
	}

	fpf_ret = fprintf(f, "%d\n", version_);
	if (fpf_ret < 0) {
		error = isi_system_error_new(errno,
		    "Failed to append to file");
		goto out;
	}

	for (std::vector<cpool_message*>::const_iterator it = messages_.begin();
	    it != messages_.end(); ++it) {

		(*it)->append_to(f, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

 out:
	if (f != NULL)
		fclose(f);

	isi_error_handle(error, error_out);
}

void
cpool_envelope::check(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int local_version = -1;
	int fields_populated = -1;
	FILE *f = fopen(get_incoming_file_name(), "r");

	if (f == NULL) {
		/* A missing file is ok.  It just means there are no messages */
		if (errno == ENOENT)
			goto out;

		error = isi_system_error_new(errno,
		    "Failed to open file \"%s\"", get_incoming_file_name());
		goto out;
	}

	fields_populated = fscanf(f, "%d\n", &local_version);

	if (fields_populated == EOF) {
		error = isi_system_error_new(errno, "Unable to read envelope");
		goto out;
	}

	if (fields_populated != 1) {
		error = isi_system_error_new(EINVAL, "Invalid envelope format");
		goto out;
	}

	if (local_version != version_) {
		error = isi_system_error_new(EINVAL, "Unsupported version");
		goto out;
	}

	while (!feof(f)) {
		cpool_message *message = new cpool_message();
		ASSERT(message != NULL);

		message->read_from(f, &error);
		if (error)
			delete message;
		ON_ISI_ERROR_GOTO(out, error);

		add_message(message);
	}

 out:

	if (f != NULL)
		fclose(f);

	isi_error_handle(error, error_out);
}

const std::vector<cpool_message*> &
cpool_envelope::get_messages() const
{
	return messages_;
}

bool
cpool_envelope::has_messages() const
{
	return get_messages().size() > 0;
}

const char *
cpool_envelope::get_outgoing_file_name() const
{
	return ENVELOPE_OUTGOING_FILE;
}

const char *
cpool_envelope::get_incoming_file_name() const
{
	return ENVELOPE_INCOMING_FILE;
}

void
cpool_envelope::delete_envelope_from_disk() const
{
	unlink(get_incoming_file_name());
}
