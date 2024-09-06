#ifndef __ISI_CPOOL_D_TASK_KEY_H__
#define __ISI_CPOOL_D_TASK_KEY_H__

#include <ifs/ifs_types.h>

#include "isi_cloud_object_id.h"

class cpool_operation_type;

/**
 * A task_key identifies a task.
 */
class task_key
{
protected:
	const cpool_operation_type	*op_type_;

	mutable bool			serialization_up_to_date_;
	mutable void			*serialization_;
	mutable size_t			serialization_size_;

	/* Default construction and assignment are not allowed. */
	task_key();
	task_key &operator=(const task_key&);

	/* Use get_serialized_key instead. */
	void serialize(void) const;

public:
	task_key(const cpool_operation_type *op_type);
	task_key(const task_key&);
	virtual ~task_key();

	const cpool_operation_type *get_op_type(void) const;

	void get_serialized_key(const void *&key, size_t &key_len) const;
	void set_serialized_key(const void *key, size_t key_len,
	    struct isi_error **error_out);

	/**
	 * Get a 64-bit identifier for this key.
	 */
	virtual uint64_t get_identifier(void) const = 0;

	virtual void fmt_conv(struct fmt *fmt,
	    const struct fmt_conv_args *args) const = 0;

	virtual bool equals(const task_key *other) const;

	virtual task_key *clone(void) const = 0;

	virtual size_t get_packed_size(void) const = 0;
	virtual void pack(void **stream) const = 0;
	virtual void unpack(void **stream, struct isi_error **error_out) = 0;

	static task_key *create_key(const cpool_operation_type *op_type,
	    const void *serialization, size_t serialization_size,
	    struct isi_error **error_out);

	virtual void get_name(char *name, size_t name_len,
	    size_t *name_len_out, struct isi_error **error_out) const = 0;
};

/**
 * A file_task_key is used where tasks can be identified by a file
 * (i.e. LIN/snapid).
 */
class file_task_key : public task_key
{
private:
	typedef task_key super;

protected:
	ifs_lin_t			lin_;
	ifs_snapid_t			snapid_;

	/* Copy construction (use clone()) and assignment are not allowed. */
	file_task_key(const file_task_key&);
	file_task_key &operator=(const file_task_key&);

public:
	file_task_key(const cpool_operation_type *op_type);
	file_task_key(const cpool_operation_type *op_type, ifs_lin_t lin,
	    ifs_snapid_t snapid);
	virtual ~file_task_key();

	void set_lin(ifs_lin_t lin);
	void set_snapid(ifs_snapid_t snapid);

	ifs_lin_t get_lin(void) const;
	ifs_snapid_t get_snapid(void) const;

	uint64_t get_identifier(void) const;

	void fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const;

	virtual bool equals(const task_key *other) const;

	virtual task_key *clone(void) const;

	size_t get_packed_size(void) const;
	void pack(void **stream) const;
	void unpack(void **stream, struct isi_error **error_out);

	void get_name(char *name, size_t name_len, size_t *name_len_out,
	    struct isi_error **error_out) const;
};

/**
 * A cloud_object_id_task_key is used where tasks can be identified by a cloud
 * object ID.
 */
class cloud_object_id_task_key : public task_key
{
private:
	typedef task_key super;

protected:
	isi_cloud_object_id		object_id_;
	bool				object_id_set_;

	mutable uint64_t		cached_identifier_;

	/* Copy construction (see clone()) and assignment are not allowed. */
	cloud_object_id_task_key(const cloud_object_id_task_key&);
	cloud_object_id_task_key &operator=(const cloud_object_id_task_key&);

public:
	cloud_object_id_task_key(const cpool_operation_type *op_type);
	cloud_object_id_task_key(const cpool_operation_type *op_type,
	    const isi_cloud_object_id &object_id);
	virtual ~cloud_object_id_task_key();

	void set_object_id(const isi_cloud_object_id &object_id);

	const isi_cloud_object_id &get_object_id(void) const;

	uint64_t get_identifier(void) const;

	void fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const;

	virtual bool equals(const task_key *other) const;

	virtual task_key *clone(void) const;

	size_t get_packed_size(void) const;
	void pack(void **stream) const;
	void unpack(void **stream, struct isi_error **error_out);

	void get_name(char *name, size_t name_len, size_t *name_len_out,
	    struct isi_error **error_out) const;
};

struct task_account_id {
	uint32_t			account_id_;
	std::string			cluster_;

	size_t get_packed_size(void) const;
	void pack(void **stream) const;
	void unpack(void **stream, struct isi_error **error_out);
	bool operator==(const task_account_id &rhs) const;
	bool operator!=(const task_account_id &rhs) const;
	const std::string &to_string(void) const;
};

/**
 * A account_task_key is used where tasks can be identified by an account
 * ID.
 */
class account_id_task_key : public task_key
{
private:
	typedef task_key super;
	mutable std::string		string_form_;

protected:
	task_account_id			account_id_;
	bool				account_id_set_;

	mutable uint64_t		cached_identifier_;

	/* Copy construction (see clone()) and assignment are not allowed. */
	account_id_task_key(const account_id_task_key&);
	account_id_task_key &operator=(const account_id_task_key&);

public:
	account_id_task_key(const cpool_operation_type *op_type);
	account_id_task_key(const cpool_operation_type *op_type,
	    const task_account_id &acct_id);
	virtual ~account_id_task_key();

	void set_account_id(const task_account_id &account_id);

	const task_account_id &get_account_id(void) const;

	uint64_t get_identifier(void) const;

	void fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args) const;

	virtual bool equals(const task_key *other) const;

	virtual task_key *clone(void) const;

	size_t get_packed_size(void) const;
	void pack(void **stream) const;
	void unpack(void **stream, struct isi_error **error_out);
	virtual void get_name(char *name, size_t name_len,
	    size_t *name_len_out, struct isi_error **error_out) const;

private:
	void update_string_form();
};

struct fmt_conv_ctx cpool_task_key_fmt(const task_key *key);

#endif // __ISI_CPOOL_D_TASK_KEY_H__
