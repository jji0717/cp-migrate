#include <set>
#include <limits.h>

#include "isi_cbm_error.h"
#include "isi_cbm_file_versions.h"
#include <isi_cpool_cbm/isi_cbm_file_versions.pb.h>
#include <isi_licensing/licensing.h>
#include <isi_ilog/ilog.h>

// Generated protobuf types for versions
typedef ::isi_cbm_pb::isi_cbm_file_version isi_cbm_file_version_pb;
typedef ::isi_cbm_pb::isi_cbm_file_versions isi_cbm_file_versions_pb;

static bool test_supported_versions_set = false;
static bool test_has_cp_license = true;

// We use a set and a special comparator thereof for this impl
struct version_num_comp {
	// Order by 'num'
	bool operator() (const isi_cbm_file_version &v1,
	    const isi_cbm_file_version &v2) const { return v1._num < v2._num;}
};
typedef std::set<isi_cbm_file_version, version_num_comp> version_set;


bool
operator==(const isi_cbm_file_version &v1, const isi_cbm_file_version &v2)
{
	return (v1._num == v2._num);
}

bool
operator<(const isi_cbm_file_version &v1, const isi_cbm_file_version &v2)
{
	return (v1._num < v2._num);
}

struct isi_cbm_file_versions
{
public:
	isi_cbm_file_versions();
	isi_cbm_file_versions(isi_cbm_file_version in_v[], size_t num);
	~isi_cbm_file_versions();

	void insert(const isi_cbm_file_version &ver);
	void remove(const isi_cbm_file_version &ver);
	void clear();
	bool exists(const isi_cbm_file_version &ver) const;

	isi_cbm_file_versions intersection(
	    const isi_cbm_file_versions &other) const;

	size_t serialize(void *buf, size_t nbytes,
	    struct isi_error **) const;
	void deserialize(const void *buf, size_t nbytes,
	    struct isi_error **);

private:
	version_set impl_;
};

isi_cbm_file_versions::isi_cbm_file_versions()
{
}

isi_cbm_file_versions::isi_cbm_file_versions(isi_cbm_file_version in_v[],
    size_t num)
{
	for (size_t i = 0 ; i < num; i++)
		impl_.insert(in_v[i]);
}

isi_cbm_file_versions::~isi_cbm_file_versions()
{
}

void
isi_cbm_file_versions::insert(const isi_cbm_file_version &ver)
{
	impl_.insert(ver);
}

void
isi_cbm_file_versions::remove(const isi_cbm_file_version &ver)
{
	impl_.erase(ver);
}

void
isi_cbm_file_versions::clear()
{
	impl_.clear();
}

bool
isi_cbm_file_versions::exists(const isi_cbm_file_version &ver) const
{
	bool found = false;

	version_set::iterator it = impl_.find(ver);

	if (it != impl_.end()) {
		ASSERT(it->_num == ver._num);
		// make sure that all of the flags are present too
		found = ((ver._flags & it->_flags) == ver._flags);
	}

	return found;
}

isi_cbm_file_versions
isi_cbm_file_versions::intersection(const isi_cbm_file_versions &that) const
{
	version_set::iterator it_this = impl_.begin();
	version_set::iterator it_that =  that.impl_.begin();

	isi_cbm_file_versions common;

	while (it_this != impl_.end() && it_that != that.impl_.end()) {
		if (*it_this == *it_that) {
			isi_cbm_file_version ver;
			ver._num = it_this->_num;
			ver._flags = it_this->_flags & it_that->_flags;

			common.insert(ver);
			++it_this;
			++it_that;

		} else if (*it_this < *it_that) {
			++it_this;
		} else {
			++it_that;
		}
	}

	return common;
}

size_t
isi_cbm_file_versions::serialize(void *buf, size_t nbytes,
    struct isi_error **error_out) const
{
	struct isi_error *error = NULL;
	size_t need_bytes = nbytes;
	isi_cbm_file_versions_pb versions;
	bool serialized = false;

	// Iterate through impl_ and set values in versions protobuf
	for (version_set::iterator it = impl_.begin(); it != impl_.end();
	    ++it) {
		isi_cbm_file_version_pb *version;

		version = versions.add_version();
		ASSERT(version);
		version->set_num(it->_num);
		version->set_flags(it->_flags);
	}

	// Serialize protobuf into stream
	ASSERT(nbytes < INT_MAX);
	serialized = versions.SerializeToArray(buf, (int) nbytes);
	need_bytes = versions.ByteSize();
	if (!serialized) {
		// check if this is because 'buf' isn't big enough
		if (nbytes < need_bytes) {
			error = isi_cbm_error_new(CBM_LIMIT_EXCEEDED,
			    " protobuf needs %lu bytes to serialize "
			     "but only had %lu bytes to work with", need_bytes,
			     nbytes);
		} else {
			error = isi_cbm_error_new(CBM_INVALID_PARAM_ERROR,
			    "protobuf serialization failure");
		}
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
	return need_bytes;
}

void
isi_cbm_file_versions::deserialize(const void *buf, size_t nbytes,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	isi_cbm_file_versions_pb versions;
	int i;

	ASSERT(nbytes < INT_MAX);
	if (!versions.ParseFromArray(buf, (int) nbytes)) {
		error = isi_cbm_error_new(CBM_INVALID_PARAM_ERROR,
		    "protobuf deserialization failure");
		goto out;
	}

	// Clear our state
	impl_.clear();

	// Iterate through versions protobuf and set values in our impl_
	for (i = 0; i < versions.version_size(); ++i) {
		const isi_cbm_file_version_pb & version = versions.version(i);

		isi_cbm_file_version ver;
		ver._num = version.num();
		ver._flags = version.flags();

		impl_.insert(ver);
	}
 out:
	isi_error_handle(error, error_out);
}

namespace {
// This the place to specify what stub file versions
// are supported and (in future deprecated) in the current release
isi_cbm_file_version v_supp[] = {
	{CBM_FILE_VER_RIPTIDE_NUM, CBM_FILE_VER_NIIJIMA_FLAGS | HAS_V4_AUTH_VER_ONLY}
	};

// The singleton supported and deprecated version sets
isi_cbm_file_versions supported_versions(v_supp,
    sizeof(v_supp)/sizeof(v_supp[0]));
isi_cbm_file_versions deprecated_versions;

}

// TEST FUNCTIONS -- BEGIN
void
supported_versions_set(isi_cbm_file_version v[], size_t num)
{
	supported_versions.clear();

	for (size_t i = 0; i < num; i++)
		supported_versions.insert(v[i]);

	test_supported_versions_set = true;
}

void
supported_versions_clear(void)
{
	supported_versions.clear();

	test_supported_versions_set = false;
}

void
isi_cbm_file_versions_license(bool available)
{
	test_has_cp_license = available;
}
// TEST FUNCTIONS -- END

struct isi_cbm_file_versions *
isi_cbm_file_versions_create(void)
{
	return new isi_cbm_file_versions();
}

struct isi_cbm_file_versions *
isi_cbm_file_versions_get_supported(void)
{
	struct isi_cbm_file_versions *vs = NULL;
	bool has_cp_license = false;

	has_cp_license =
			isi_licensing_module_status(ISI_LICENSING_CLOUDPOOLS) == ISI_LICENSING_LICENSED;

	// Return the stub file versions we support
	if (test_has_cp_license &&
	    (has_cp_license || test_supported_versions_set)) {
		vs = new isi_cbm_file_versions(supported_versions);
	} else {
		vs = new isi_cbm_file_versions(); // empty version set
		ilog(IL_DEBUG, "CP license not available, no cbm file versions "
		    "are supported"); 
	}

	return vs;
}

void
isi_cbm_file_versions_destroy(struct isi_cbm_file_versions *vs)
{
	delete vs;
}

struct isi_cbm_file_versions *
isi_cbm_file_versions_common(
    const struct isi_cbm_file_versions *vs1,
    const struct isi_cbm_file_versions *vs2)
{
	const struct isi_cbm_file_versions empty_set;

	const struct isi_cbm_file_versions *a = vs1 ? vs1 : &empty_set;
	const struct isi_cbm_file_versions *b = vs2 ? vs2 : &empty_set;

	if (a == b)
		return new isi_cbm_file_versions(*a);

	return new isi_cbm_file_versions(a->intersection(*b));
}

size_t
isi_cbm_file_versions_serialize(const struct isi_cbm_file_versions *vs,
    void *buf, size_t nbytes, struct isi_error **error_out)
{
	ASSERT(vs && buf && error_out);
	return vs->serialize(buf, nbytes, error_out);

}

struct isi_cbm_file_versions *
isi_cbm_file_versions_deserialize(const void *buf, size_t nbytes,
    struct isi_error **error_out)
{
	ASSERT((!!buf == !!nbytes) && error_out);
	struct isi_cbm_file_versions *vs = isi_cbm_file_versions_create();

	if (nbytes) {
		vs->deserialize(buf, nbytes, error_out);

		if (*error_out) {
			delete vs;
			vs = NULL;
		}
	}
	return vs;
}

bool
isi_cbm_file_version_exists(const struct isi_cbm_file_version *v,
    const struct isi_cbm_file_versions *vs)
{
	ASSERT(v && vs);

	return vs->exists(*v);
}

size_t
isi_cbm_file_version_serialize(const struct isi_cbm_file_version *v,
    void *buf, size_t nbytes, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	size_t need_bytes = nbytes;
	isi_cbm_file_version_pb version_pb;
	bool serialized = false;

	ASSERT(v && buf && error_out);

	version_pb.set_num(v->_num);
	version_pb.set_flags(v->_flags);

	ASSERT(nbytes < INT_MAX);
	serialized = version_pb.SerializeToArray(buf, (int) nbytes);
	need_bytes = version_pb.ByteSize();
	if (!serialized) {
		// check if this is because 'buf' isn't big enough
		if (nbytes < need_bytes) {
			error = isi_cbm_error_new(CBM_LIMIT_EXCEEDED,
			    " protobuf needs %lu bytes to serialize "
			     "but only had %lu bytes to work with", need_bytes,
			     nbytes);
		} else {
			error = isi_cbm_error_new(CBM_INVALID_PARAM_ERROR,
			    "protobuf serialization failure");
		}
		goto out;
	}

 out:
	isi_error_handle(error, error_out);
	return need_bytes;
}

void 
isi_cbm_file_version_deserialize(struct isi_cbm_file_version *v,
    const void *buf, size_t nbytes,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	isi_cbm_file_version_pb version_pb;

	ASSERT(v && buf && error_out);

	if (!nbytes || !version_pb.ParseFromArray(buf, (int) nbytes)) {
		error = isi_cbm_error_new(CBM_INVALID_PARAM_ERROR,
		    "protobuf deserialization failure");
		goto out;
	}

	v->_num = version_pb.num();
	v->_flags = version_pb.flags();

 out:
	isi_error_handle(error, error_out);
}
