#ifndef __ISI_CPOOL_D_CPOOL_MESSAGE_H__
#define __ISI_CPOOL_D_CPOOL_MESSAGE_H__

#include <vector>
#include "cpool_api_c.h"

enum cpool_message_action {
	CPM_ACTION_NONE,
	CPM_ACTION_NOTIFY
};

#define CPOOL_MESSAGE_VERSION 1
#define CPOOL_ENVELOPE_VERSION 1

/**
 * cpool_message and cpool_envelope are a simple, file-based mechanism for
 * sending messages between different daemons and different nodes.  The current
 * implementation works in tandem with MCP as follows:
 *   - A cpool_envelope is created and populated with one or more cpool_messages
 *   - The envelope (and its contents) are written to a file
 *   - MCP monitors the envelope file.  When the file changes, MCP copies it to
 *     a temporary directory on each node and sends a SIGUSR1 to each node's
 *     running cpool daemon
 *   - The cpool daemon on each node responds to the signal by looking for
 *     messages (e.g., creating a new cpool_envelope and executing 'check')
 *   - When the daemon is done processing the envelope, the daemon may (should)
 *     delete it from disk (e.g., cpool_envelope::delete_envelope_from_disk)
 *
 * NOTE_1* these classes are not inherently thread safe
 * NOTE_2* messages are not guaranteed to be delivered and should not be used
 *         for critical operations (e.g., new envelopes can overwrite existing
 *         envelopes)
 */
class cpool_message
{
public:
	cpool_message() : recipient_(CPM_RECIPIENT_NONE),
	    action_(CPM_ACTION_NONE), version_(CPOOL_MESSAGE_VERSION) {};

	cpool_message(cpool_message_recipient to, cpool_message_action what) :
	    recipient_(to), action_(what), version_(CPOOL_MESSAGE_VERSION) {};

	/**
	 * Serializes this message to an open file
	 *
	 * @param[in] f			A pointer to an open FILE
	 * @param[out] error_out	Standard error handler
	 */
	void append_to(FILE *f, struct isi_error **error_out) const;

	/**
	 * Reads the given file to populate/deserialize current message
	 * instance.  Assumes the file position is at the correct position to
	 * begin reading
	 *
	 * @param[in] f			A pointer to an open FILE
	 * @param[out] error_out	Standard error handler
	 */
	void read_from(FILE *f, struct isi_error **error_out);

	/**
	 * Returns the intended recipient of this message
	 */
	inline cpool_message_recipient get_recipient() const
	{
		return recipient_;
	};

	/**
	 * Returns the intended action of this message
	 */
	inline cpool_message_action get_action() const
	{
		return action_;
	};

private:
	cpool_message_recipient recipient_;
	cpool_message_action action_;
	int version_;
};

class cpool_envelope
{
public:
	cpool_envelope() : version_(CPOOL_ENVELOPE_VERSION) {};
	virtual ~cpool_envelope();

	/**
	 * Creates and adds a message to this envelope with the given
	 * information
	 *
	 * @param[in] to	The intended recipient of the message
	 * @param[in] what	The intended action of the message
	 */
	void add_message(cpool_message_recipient to, cpool_message_action what);

	/**
	 * Adds the given message to this envelope
	 *
	 * @param[in] message	A pointer to the message being added to this
	 *			envelope.  (Note* envelope assumes ownership of
	 *			message pointer.  It is an error for a caller
	 *			to deallocate the given message instance.)
	 */
	void add_message(cpool_message *message);

	/**
	 * "Sends" this envelope to the cpool daemon by serializing to file
	 *
	 * @param[out] error_out	Standard error handler
	 */
	void send(struct isi_error **error_out) const;

	/**
	 * Checks for incoming messages by deserializing file.  Does not return
	 * an error for non-existent messages (e.g., swallows ENOENT)
	 *
	 * @param[out] error_out	Standard error handler
	 */
	void check(struct isi_error **error_out);

	/**
	 * Returns a reference to a vector of messages
	 */
	const std::vector<cpool_message*> &get_messages() const;

	/**
	 * Returns true if there are any messages in this envelope and false
	 * otherwise.
	 */
	bool has_messages() const;

	/**
	 * Removes this envelope's incoming file from disk (e.g., ostensibly
	 * to protect against double processing an envelope
	 */
	void delete_envelope_from_disk() const;

protected:
	/* Made virtual for the sake of overriding in unit tests */
	virtual const char *get_outgoing_file_name() const;
	virtual const char *get_incoming_file_name() const;

private:
	std::vector<cpool_message*> messages_;
	int version_;
};

#endif // __ISI_CPOOL_D_CPOOL_MESSAGE_H__
