#include <sstream>
#include <ifs/ifs_syscalls.h>
#include <ifs/bam/bam_pctl.h>

#include <isi_util/isi_hash.h>
#include <isi_util/isi_error.h>

#include "operation_type.h"

#include "task_key.h"

static inline void
pack(void **dst, const void *src, int size)
{
	memcpy(*dst, src, size);
	*(char **)dst += size;
}

static inline void
unpack(void **dst, void *src, int size)
{
	memcpy(src, *dst, size);
	*(char **)dst += size;
}

static void
task_key_fmt_conv(struct fmt *fmt, const struct fmt_conv_args *args,
    union fmt_conv_arg arg)
{
	const task_key *key = static_cast<const task_key *>(arg.ptr);
	ASSERT(key != NULL);

	key->fmt_conv(fmt, args);
}

struct fmt_conv_ctx
cpool_task_key_fmt(const task_key *key)
{
	struct fmt_conv_ctx ctx = { task_key_fmt_conv, { ptr: key } };
	return ctx;
}

task_key::task_key(const cpool_operation_type *op_type)
	: op_type_(op_type), serialization_up_to_date_(false)
	, serialization_(NULL), serialization_size_(0)
{
	ASSERT(op_type != NULL);
	ASSERT(op_type != OT_NONE);
}

task_key::task_key(const task_key &rhs)
	: op_type_(rhs.op_type_), serialization_up_to_date_(false)
	, serialization_(NULL), serialization_size_(0)
{
}

task_key::~task_key()
{
	free(serialization_);
}

const cpool_operation_type *
task_key::get_op_type(void) const
{
	return op_type_;
}

void
task_key::get_serialized_key(const void *&key, size_t &key_len) const
{
	if (!serialization_up_to_date_)
		serialize();

	key = serialization_;
	key_len = serialization_size_;
}

void
task_key::set_serialized_key(const void *key, size_t key_len,
    struct isi_error **error_out)
{
	ASSERT(key != NULL);

	struct isi_error *error = NULL;

	serialization_size_ = key_len;

	free(serialization_);
	serialization_ = malloc(serialization_size_);
	ASSERT(serialization_ != NULL);

	memcpy(serialization_, key, serialization_size_);

	void *temp = serialization_;
	unpack(&temp, &error);
	ON_ISI_ERROR_GOTO(out, error);

	serialization_up_to_date_ = true;

 out:
	isi_error_handle(error, error_out);
}

void
task_key::serialize(void) const
{
	free(serialization_);

	serialization_size_ = get_packed_size();
	serialization_ = malloc(serialization_size_);
	ASSERT(serialization_ != NULL);

	void *temp = serialization_;
	pack(&temp);

	serialization_up_to_date_ = true;
}

bool
task_key::equals(const task_key *other) const
{
	return ((other != NULL) && (get_op_type() == other->get_op_type()));
}

task_key *
task_key::create_key(const cpool_operation_type *op_type,
    const void *serialization, size_t serialization_size,
    struct isi_error **error_out)
{
	ASSERT(op_type != NULL);
	ASSERT(op_type != OT_NONE);

	struct isi_error *error = NULL;
	task_key *new_key = NULL;

	if (op_type == OT_CLOUD_GC)
		new_key = new cloud_object_id_task_key(op_type);
	else
		new_key = new file_task_key(op_type);
	ASSERT(new_key != NULL);

	new_key->set_serialized_key(serialization, serialization_size, &error);
	ON_ISI_ERROR_GOTO(out, error);

 out:
	isi_error_handle(error, error_out);

	return new_key;
}

file_task_key::file_task_key(const cpool_operation_type *op_type)
	: super(op_type), lin_(INVALID_LIN), snapid_(INVALID_SNAPID)
{
}

file_task_key::file_task_key(const cpool_operation_type *op_type,
    ifs_lin_t lin, ifs_snapid_t snapid)
	: super(op_type), lin_(lin), snapid_(snapid)
{
}

file_task_key::file_task_key(const file_task_key &rhs)
	: super(rhs), lin_(rhs.lin_), snapid_(rhs.snapid_)
{
}

file_task_key::~file_task_key()
{
}

void
file_task_key::set_lin(ifs_lin_t lin)
{
	ASSERT(lin != INVALID_LIN);

	lin_ = lin;
	serialization_up_to_date_ = false;
}

void
file_task_key::set_snapid(ifs_snapid_t snapid)
{
	ASSERT(snapid != INVALID_SNAPID);

	snapid_ = snapid;
	serialization_up_to_date_ = false;
}

ifs_lin_t
file_task_key::get_lin(void) const
{
	return lin_;
}

ifs_snapid_t
file_task_key::get_snapid(void) const
{
	return snapid_;
}

uint64_t
file_task_key::get_identifier(void) const
{
	ASSERT(lin_ != INVALID_LIN);
	ASSERT(snapid_ != INVALID_SNAPID);

	return lin_;
}

void
file_task_key::fmt_conv(struct fmt *fmt,
    const struct fmt_conv_args *args) const
{
	fmt_print(fmt, "%{}/%{}",
	    task_type_fmt(op_type_->get_type()),
	    lin_snapid_fmt(get_lin(), get_snapid()));
}

bool
file_task_key::equals(const task_key *other) const
{
	bool equivalent = false;
	const file_task_key *file_key = NULL;

	if (other == NULL)
		goto out;

	file_key = dynamic_cast<const file_task_key *>(other);
	if (file_key == NULL)
		goto out;

	if (!super::equals(other))
		goto out;

	if (get_lin() != file_key->get_lin())
		goto out;

	if (get_snapid() != file_key->get_snapid())
		goto out;

	equivalent = true;

 out:
	return equivalent;
}

task_key *
file_task_key::clone(void) const
{
	return new file_task_key(*this);
}

size_t
file_task_key::get_packed_size(void) const
{
	return sizeof(ifs_lin_t) + sizeof(ifs_snapid_t);
}

void
file_task_key::pack(void **stream) const
{
	ASSERT(lin_ != INVALID_LIN);
	ASSERT(snapid_ != INVALID_SNAPID);
	ASSERT(stream != NULL);

	::pack(stream, &lin_, sizeof lin_);
	::pack(stream, &snapid_, sizeof snapid_);
}

void
file_task_key::unpack(void **stream, struct isi_error **error_out)
{
	ASSERT(stream != NULL);

	::unpack(stream, &lin_, sizeof lin_);
	::unpack(stream, &snapid_, sizeof snapid_);
}

void
file_task_key::get_name(char *name, size_t name_len, size_t *name_len_out,
    struct isi_error **error_out) const
{
	ASSERT(name != NULL);
	ASSERT(name_len >= 6, "name buffer too small (%zu)", name_len);

	struct isi_error *error = NULL;
	const char *prefix = "/ifs/";
	int prefix_len = strlen(prefix);

	/* Prepend "/ifs/" to the file name. */
	strcpy(name, prefix);

	if (pctl2_lin_get_path_plus(0, lin_, snapid_, 0, 0, ENC_DEFAULT,
		name_len - prefix_len, name + prefix_len, name_len_out,
		0, NULL, NULL, 0, NULL, NULL,
		PCTL2_LIN_GET_PATH_NO_PERM_CHECK) != 0) {  // bug 220379
		error = isi_system_error_new(errno,
		    "failed to retrieve path for LIN/snap %{}",
		    lin_snapid_fmt(lin_, snapid_));
		goto out;
	}

 out:
	/* The name_len_out is valid in the case of success and ENOSPC error.*/
	if (name_len_out != NULL) {
		*name_len_out += prefix_len;
	}
	isi_error_handle(error, error_out);
}

cloud_object_id_task_key::cloud_object_id_task_key(
    const cpool_operation_type *op_type)
	: super(op_type), object_id_set_(false), cached_identifier_(0)
{
}

cloud_object_id_task_key::cloud_object_id_task_key(
    const cpool_operation_type *op_type, const isi_cloud_object_id &object_id)
	: super(op_type), object_id_(object_id), object_id_set_(true)
	, cached_identifier_(0)
{
}

cloud_object_id_task_key::cloud_object_id_task_key(
    const cloud_object_id_task_key &rhs)
	: super(rhs), object_id_(rhs.object_id_)
	, object_id_set_(rhs.object_id_set_)
	, cached_identifier_(rhs.cached_identifier_)
{
}

cloud_object_id_task_key::~cloud_object_id_task_key()
{
}

void
cloud_object_id_task_key::set_object_id(const isi_cloud_object_id &object_id)
{
	object_id_ = object_id;
	object_id_set_ = true;
	cached_identifier_ = 0;
}

const isi_cloud_object_id &
cloud_object_id_task_key::get_object_id(void) const
{
	return object_id_;
}

uint64_t
cloud_object_id_task_key::get_identifier(void) const
{
	ASSERT(object_id_set_, "key not initialized with object ID");

	/*
	 * We're using 0 as the uninitialized value; in the (very unlikely)
	 * case where a cloud object ID is used such that it hashes to 0 then
	 * we do a little extra work but still have correct behavior.
	 */
	if (cached_identifier_ == 0) {
		void *object_id = malloc(object_id_.get_packed_size());
		ASSERT(object_id != NULL);

		void *temp = object_id;
		object_id_.pack(&temp);

		cached_identifier_ = isi_hash64(object_id,
		    object_id_.get_packed_size(), 0);

		free(object_id);
	}

	return cached_identifier_;
}

void
cloud_object_id_task_key::fmt_conv(struct fmt *fmt,
    const struct fmt_conv_args *args) const
{
	fmt_print(fmt, "%{}/%s",
	    task_type_fmt(op_type_->get_type()),
	    get_object_id().to_c_string());
}

bool
cloud_object_id_task_key::equals(const task_key *other) const
{
	bool equivalent = false;

	const cloud_object_id_task_key *coid_key =
	    dynamic_cast<const cloud_object_id_task_key *>(other);
	if (other == NULL)
		goto out;

	if (!super::equals(other))
		goto out;

	if (get_object_id() != coid_key->get_object_id())
		goto out;

	equivalent = true;

 out:
	return equivalent;
}

task_key *
cloud_object_id_task_key::clone(void) const
{
	return new cloud_object_id_task_key(*this);
}

size_t
cloud_object_id_task_key::get_packed_size(void) const
{
	return isi_cloud_object_id::get_packed_size();
}

void
cloud_object_id_task_key::pack(void **stream) const
{
	ASSERT(object_id_set_, "key not initialized with object ID");
	ASSERT(stream != NULL);

	object_id_.pack(stream);
}

void
cloud_object_id_task_key::unpack(void **stream, struct isi_error **error_out)
{
	ASSERT(stream != NULL);

	object_id_.unpack(stream);
	object_id_set_ = true;
}

account_id_task_key::account_id_task_key(
    const cpool_operation_type *op_type)
	: super(op_type), account_id_set_(false), cached_identifier_(0)
{
}

account_id_task_key::account_id_task_key(
    const cpool_operation_type *op_type, const task_account_id &account_id)
	: super(op_type), account_id_(account_id), account_id_set_(true)
	, cached_identifier_(0)
{
}

account_id_task_key::account_id_task_key(
    const account_id_task_key &rhs)
	: super(rhs), account_id_(rhs.account_id_)
	, account_id_set_(rhs.account_id_set_)
	, cached_identifier_(rhs.cached_identifier_)
{
	update_string_form();
}

account_id_task_key::~account_id_task_key()
{
}

void
account_id_task_key::set_account_id(const task_account_id &account_id)
{
	account_id_ = account_id;
	account_id_set_ = true;
	cached_identifier_ = 0;
	update_string_form();
}

const task_account_id &
account_id_task_key::get_account_id(void) const
{
	return account_id_;
}

uint64_t
account_id_task_key::get_identifier(void) const
{
	ASSERT(account_id_set_, "key not initialized with account ID");

	/*
	 * We're using 0 as the uninitialized value; in the (very unlikely)
	 * case where a cloud account ID is used such that it hashes to 0 then
	 * we do a little extra work but still have correct behavior.
	 */
	if (cached_identifier_ == 0) {
		void *account_id = malloc(account_id_.get_packed_size());
		ASSERT(account_id != NULL);

		void *temp = account_id;
		account_id_.pack(&temp);

		cached_identifier_ = isi_hash64(account_id,
		    account_id_.get_packed_size(), 0);

		free(account_id);
	}

	return cached_identifier_;
}

void
account_id_task_key::fmt_conv(struct fmt *fmt,
    const struct fmt_conv_args *args) const
{
	fmt_print(fmt, "%{}/(%d %s)",
	    task_type_fmt(op_type_->get_type()),
	    account_id_.account_id_, account_id_.cluster_.c_str());
}

bool
account_id_task_key::equals(const task_key *other) const
{
	bool equivalent = false;

	const account_id_task_key *acct_key =
	    dynamic_cast<const account_id_task_key *>(other);
	if (other == NULL)
		goto out;

	if (!super::equals(other))
		goto out;

	if (get_account_id() != acct_key->get_account_id())
		goto out;

	equivalent = true;

 out:
	return equivalent;
}

task_key *
account_id_task_key::clone(void) const
{
	return new account_id_task_key(*this);
}

size_t
account_id_task_key::get_packed_size(void) const
{
	return account_id_.get_packed_size();
}

void
account_id_task_key::pack(void **stream) const
{
	ASSERT(account_id_set_, "key not initialized with account ID");
	ASSERT(stream != NULL);

	account_id_.pack(stream);
}

void
account_id_task_key::update_string_form()
{
	std::stringstream strm;

	strm << "(" << get_account_id().account_id_ << ","
	    << get_account_id().cluster_
	    << ")";
	string_form_ = strm.str();	
}

void
account_id_task_key::unpack(void **stream, struct isi_error **error_out)
{
	ASSERT(stream != NULL);

	account_id_.unpack(stream, error_out);
	account_id_set_ = true;
	update_string_form();
}

void
account_id_task_key::get_name(char *name, size_t name_len,
    size_t *name_len_out, struct isi_error **error_out) const
{
	ASSERT(name != NULL);
	ASSERT(account_id_set_, "key not initialized with account ID");

	struct isi_error *error = NULL;

	if (name_len_out != NULL)
		*name_len_out = string_form_.length();

	if (name_len < string_form_.length() + 1) {
		error = isi_system_error_new(ENOSPC,
		    "supplied buffer length (%zu) is too small for name "
		    "(requires %zu)",
		    name_len, string_form_.length() + 1);
		goto out;
	}

	strncpy(name, string_form_.c_str(),
	    string_form_.length());
	name[string_form_.length()] = '\0';
 out:
	isi_error_handle(error, error_out);
}

size_t
task_account_id::get_packed_size() const
{
	return sizeof(account_id_) + sizeof(uint32_t) + cluster_.size();
}

void
task_account_id::pack(void **stream) const
{
	::pack(stream, &account_id_, sizeof(account_id_));
	uint32_t sz = cluster_.size();
	::pack(stream, &sz, sizeof(sz));
	::pack(stream, (cluster_.c_str()), cluster_.size());
}

void
task_account_id::unpack(void **stream, struct isi_error **error_out)
{
	ASSERT(stream != NULL);
	uint32_t sz = 0;
	::unpack(stream, &account_id_, sizeof(account_id_));
	::unpack(stream, &sz, sizeof(sz));
	if (sz > 0) {
		cluster_.resize(sz);
		cluster_.assign((char *)*stream, sz);
		*(char **)stream += sz;
	}
}

bool
task_account_id::operator==(const task_account_id& rhs) const
{
	return ((account_id_ == rhs.account_id_) &&
	    cluster_ == rhs.cluster_);
}

bool
task_account_id::operator!=(const task_account_id& rhs) const
{
	return !(operator==(rhs));
}

void
cloud_object_id_task_key::get_name(char *name, size_t name_len,
    size_t *name_len_out, struct isi_error **error_out) const
{
	ASSERT(name != NULL);
	ASSERT(object_id_set_, "key not initialized with object ID");

	struct isi_error *error = NULL;

	if (name_len_out != NULL)
		*name_len_out = object_id_.to_string().length();

	if (name_len < object_id_.to_string().length() + 1) {
		error = isi_system_error_new(ENOSPC,
		    "supplied buffer length (%zu) is too small for name "
		    "(requires %zu)",
		    name_len, object_id_.to_string().length() + 1);
		goto out;
	}

	strncpy(name, object_id_.to_string().c_str(),
	    object_id_.to_string().length());
	name[object_id_.to_string().length()] = '\0';

 out:
	isi_error_handle(error, error_out);
}
