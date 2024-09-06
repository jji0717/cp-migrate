#include "query_handler.h"

#include <isi_util_cpp/http_util.h>
#include <isi_persona/persona_util.h>
#include <isi_rest_server/api_utils.h>
#include <isi_rest_server/resume_token.h>
#include <isi_util_cpp/array_count.h>
#include <isi_util_cpp/first_n_ptr_set.hpp>
#include <isi_util_cpp/isi_cmp.h>
#include <isi_object/iobj_ostore_protocol.h>

#include "handler_common.h"
#include "oapi_config.h"
#include "oapi/oapi_acct.h"
#include "oapi/oapi_bucket.h"
#include "object_query.h"
#include "store_provider.h"

#define PRM_MAX_KEYS	"limit"
#define PRM_DETAIL	"detail"
#define PRM_RESUME	"resume"
#define PRM_SORT	"sort"
#define PRM_DIR		"dir"
#define PRM_TYPE	"type"
#define PRM_HIDDEN	"hidden"
#define PRM_DEPTH	"max-depth"

#define MAX_SORT_LIMIT		1000
#define DEFAULT_MAX_KEYS	1000
#define USER_NAME_LEN		256
#define MAX_DEPTH		256

const std::string TT_CONTAINER("container");
const std::string TT_OBJECT("object");
const std::string TT_STUB("stub");
const std::string TT_FIFO("pipe");
const std::string TT_CHR("character_device");
const std::string TT_BLK("block_device");
const std::string TT_LNK("symbolic_link");
const std::string TT_SOCK("socket");
const std::string TT_WHT("whiteout_file");

const std::string TT_ALL("all");
const std::string TT_ANY("any");
const std::string TT_UNKNOWN("unknown");

namespace {

const unsigned GET_TOKEN_VERSION        = 4;

const field_set type_fields[] = {
	{ OOT_INVALID,		TT_UNKNOWN.c_str() },
	{ OOT_CONTAINER,	TT_CONTAINER.c_str() },
	{ OOT_OBJECT,		TT_OBJECT.c_str() },
	{ OOT_STUB,		TT_STUB.c_str() },
	{ OOT_FIFO,		TT_FIFO.c_str() },
	{ OOT_CHR,		TT_CHR.c_str() },
	{ OOT_BLK,		TT_BLK.c_str() },
	{ OOT_LNK,		TT_LNK.c_str() },
	{ OOT_SOCK,		TT_SOCK.c_str() },
	{ OOT_WHT,		TT_WHT.c_str() },
	{ OOT_ALL,		TT_ALL.c_str() },
};

const unsigned type_fields_cnt = isi::util::array_count(type_fields);

arg_to_field arg_to_type_field(type_fields, type_fields_cnt);

class file_sorter {
public:
	file_sorter(const std::list<sort_field> &sort_info)
	    : sort_info_(sort_info)
	{ }

	// comparator based on the sort information and two object x and y
	bool operator()(const jdt_object *x, const jdt_object *y) const;

private:
	const std::list<sort_field> &sort_info_;
};

typedef first_n_ptr_set<jdt_object, file_sorter> file_ptr_set;

const size_t MAX_CACHE_SIZE = 256;

// simple cached query name loader
class user_name_loader {
public:
	explicit user_name_loader(size_t max_size = MAX_CACHE_SIZE)
	    : max_size_(max_size) { }

	void load_user_name(std::string &name, uid_t uid);
private:
	std::map<uid_t, std::string> id_name_map_;
	size_t max_size_;
};

// simple cached query name loader
class group_name_loader {
public:
	explicit group_name_loader(size_t max_size = MAX_CACHE_SIZE)
	    : max_size_(max_size) { }

	void load_group_name(std::string &name, gid_t gid);
private:
	std::map<gid_t, std::string> id_name_map_;
	size_t max_size_;
};

/**
 * resume cookie in a tree
 */
struct resume_cookie {
	std::vector<uint64_t> cookies_;
};

struct list_base_context;
/**
 * function prototype to write an entry from list operation
 */
typedef void (*write_an_entry)(Json::Value &objectVal,
    struct list_base_context *ctx);

void write_ns_store_list_entry(Json::Value &objectVal,
    struct list_base_context *ctx);
void write_object_list_entry(Json::Value &objectVal,
    struct list_base_context *ctx);

/**
 * base context class for list operation callback
 */
struct list_base_context {
	const request &input_;
	const object_query &query_;
	response &output_;
	bool first_;
	bool had_error_;
	bool has_more_;
	uint64_t keys_written_;
	file_ptr_set *file_set_;
	resume_output_token token_;
	jdt_object resume_key_;
	const attribute_manager *attr_mgr_;
	uint64_t traversed_cnt_;
	bool exceeded_limit_;
	write_an_entry writer_;
	list_base_context(const request &input,
	    const object_query &query,
	    response &output, write_an_entry writer);
	~list_base_context();

};
list_base_context::list_base_context(
    const request &input, const object_query &query,
    response &output, write_an_entry writer)
    : input_(input), query_(query), output_(output), first_(true),
    had_error_(false), has_more_(false), keys_written_(0),
    file_set_(NULL), token_(GET_TOKEN_VERSION),
    resume_key_(idt_class::instance()),
    attr_mgr_(NULL),
    traversed_cnt_(NULL),
    exceeded_limit_(false),
    writer_(writer)
{
}

list_base_context::~list_base_context()
{
	if (file_set_)
		delete file_set_;
}
/**
 * The listing object callback context
 */
struct list_object_context : public list_base_context {
	std::string bucket_;
	user_name_loader user_loader_;
	group_name_loader group_loader_;
	std::string bucket_path_;
	const oapi_store &store_;
	int max_depth_;
	list_object_context(const request &input,
	    const object_query &query,
	    response &output,
	    const std::string &bucket,
	    const std::string &bucket_path,
	    const oapi_store &store);
};

list_object_context::list_object_context(
    const request &input, const object_query &query,
    response &output, const std::string &bucket,
    const std::string &bucket_path,
    const oapi_store &store)
    : list_base_context(input, query, output, &write_object_list_entry),
    bucket_(bucket),
    bucket_path_(bucket_path),
    store_(store)
{
}


struct list_obj_param {
	iobj_list_obj_param param_;
	list_obj_param();
	~list_obj_param();
};

list_obj_param::list_obj_param()
{
	memset(&param_, 0, sizeof(param_));
}

list_obj_param::~list_obj_param()
{
	if (param_.resume_cookie != NULL)
		delete [] param_.resume_cookie;
}

bool
compare_field(const jdt_object *x, const jdt_object *y,
    const sort_field &field, bool &determined)
{
	//in order to implement a strict weak ordering the comparison operator
	//must be irreflexive. (i.e. we need to return false when x = y)
	bool first = false;
	const Json::Value *val_x = x->get_attr(field.field_);
	const Json::Value *val_y = y->get_attr(field.field_);

	if (val_x == NULL && val_y != NULL) {
		first = field.dir_ == SORT_DIR_ASC ? true : false;
		determined = true;
	}
	else if (val_x != NULL && val_y == NULL) {
		first = field.dir_ == SORT_DIR_ASC ? false : true;
		determined = true;
	}
	else if (val_x != NULL && val_y != NULL)
	{
		int c = val_x->compare(*val_y);

		if (c != 0) {
			first = field.dir_ == SORT_DIR_ASC ? c < 0 : c > 0;
			determined = true;
		}
	}
	
	return first;
}

/**
 * compare operator which decides if x should appear before y
 */
bool
file_sorter::operator()(const jdt_object *x, const jdt_object *y) const
{
	bool first = true;
	bool determined = false;

	size_t sz = sort_info_.size();
	assert(sz > 0 );
	std::list<sort_field>::const_iterator it;
	for (it = sort_info_.begin(); !determined && it != sort_info_.end();
	    ++it) {
		const sort_field &field = *it;
		first = compare_field(x, y, field, determined);
	}
	
	if (!determined) {
		// still tied, use the file number to decide
		// This may not be enough though, (bug 156549)
		sort_field field = {ISI_ID, SORT_DIR_ASC};
		first = compare_field(x, y, field, determined);
	}
	
	return first;
}


resume_input_token &
operator>>(resume_input_token &rit, jdt_object &info)
{
	Json::Value &objectVal = info.get_json();

	std::string val;
	rit >> val;
	Json::Reader reader;

	if (!reader.parse(val, objectVal) || !objectVal.isObject()) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid resume token.");
	}

	return rit;
}

resume_output_token &
operator<<(resume_output_token &rot, const jdt_object &info)
{
	const Json::Value &objectVal = info.get_json();

	rot << objectVal.toStyledString();
	return rot;
}

resume_input_token &
operator>>(resume_input_token &rit, resume_cookie &cookie)
{
	int count;
	rit >> count;

	if (count > MAX_DEPTH || count < 0) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Invalid resume token.");
	}
	cookie.cookies_.reserve(count);

	for (int i = 0; i < count; ++i) {
		uint64_t elem = 0;
		rit >> elem;
		cookie.cookies_.push_back(elem);
	}
	return rit;
}

resume_output_token &
operator<<(resume_output_token &rot, const resume_cookie &cookie)
{
	int count = cookie.cookies_.size();

	if (count > MAX_DEPTH || count < 0) {
		throw api_exception(AEC_BAD_REQUEST,
		    "Unsupported tree depth: %d, maximum: %d.",
		    count, MAX_DEPTH);
	}
	rot << count;

	for (int i = 0; i < count; ++i)
		rot << cookie.cookies_[i];

	return rot;
}

void
user_name_loader::load_user_name(std::string &name, uid_t uid)
{
	std::map<uid_t, std::string>::const_iterator it;
	it = id_name_map_.find(uid);

	if (it != id_name_map_.end()) {
		name = it->second;
		return;
	}
	query_util::get_user_name_from_uid(name, uid);

	// simplistic method to avoid the unlikely excessive memory usage:
	if (id_name_map_.size() < max_size_)
		id_name_map_[uid] = name;
}

void
group_name_loader::load_group_name(std::string &name, gid_t gid)
{
	std::map<uid_t, std::string>::const_iterator it;
	it = id_name_map_.find(gid);

	if (it != id_name_map_.end()) {
		name = it->second;
		return;
	}
	query_util::get_group_name_from_gid(name, gid);

	// simplistic method to avoid the unlikely excessive memory usage:
	if (id_name_map_.size() < max_size_)
		id_name_map_[gid] = name;
}

/**
 * Set the ISI_OWNER in the JSon value from the uid
 */
void
set_user_name_from_uid(user_name_loader &user_loader,
    Json::Value &objectVal, const std::string &attr,
    uid_t uid)
{
	std::string user_name;
	user_loader.load_user_name(user_name, uid);
	objectVal[attr] = user_name;
}

/**
 * Set the ISI_OWNER in the JSon value from the uid
 */
void
set_group_name_from_gid(group_name_loader &group_loader,
    Json::Value &objectVal, const std::string &attr,
    gid_t gid)
{
	std::string group_name;
	group_loader.load_group_name(group_name, gid);
	objectVal[attr] = group_name;
}

void
write_list_value_entry(Json::Value &objectVal, list_base_context *ctx,
    const char *arr_name)
{
	if (ctx->first_) {
		// we are writing a json text manually:
		ctx->output_.set_content_type(
		    api_content::APPLICATION_JSON);

		// write the array specifier "["
		std::string begin("{\"");
		begin.append(arr_name);
		begin.append("\":[");
		ctx->output_.write_data(begin.c_str(), begin.size());

		ctx->first_ = false;
		if (objectVal.isNull())
			return;
	}
	else {
		ctx->output_.write_data(",", sizeof(",") - 1);
	}
	std::string obj = objectVal.toStyledString();
	ctx->output_.write_data(obj.c_str(), obj.size());
	++ctx->keys_written_;
}

void
write_ns_store_list_entry(Json::Value &objectVal, list_base_context *ctx)
{
	return write_list_value_entry(objectVal, ctx, ATTR_NS "s");
}

void
write_object_list_entry(Json::Value &objectVal, list_base_context *ctx)
{
	// after evaluating the object we need to
	// honor the selection list.

	Json::Value::Members members = objectVal.getMemberNames();

	for (size_t i = 0; i < members.size(); ++i) {
		std::string &mem_name = members[i];
		if (!ctx->query_.is_attr_selected(mem_name)) {
			objectVal.removeMember(mem_name);
			continue;
		}
		if (query_util::is_date_attr(mem_name)) {
			// convert time to string
			std::string mod_time;
			const time_t t = objectVal[mem_name].asInt64();
			convert_time_to_http_date(t,
			    mod_time);
			objectVal[mem_name] = mod_time;
		}
	}
	write_list_value_entry(objectVal, ctx, "children");
}


/**
 * Check if we need to load the attribute.
 * If the attribute is selected in the result or needed for sorting.
 * return true.
 */
inline bool
to_load_sys_attr(list_object_context *ctx, const char *attr)
{
	const object_query &query = ctx->query_;
	return query.is_attr_used(attr);
}

/**
 * Given the mode, get the object type:
 */
const std::string &
get_object_type(mode_t mode)
{
	if (S_ISDIR(mode))
		return TT_CONTAINER;
	else if (S_ISREG(mode))
		return TT_OBJECT;
	else if (S_ISSOCK(mode))
		return TT_SOCK;
	else if (S_ISFIFO(mode))
		return TT_FIFO;
	else if (S_ISLNK(mode))
		return TT_LNK;
	else if (S_ISCHR(mode))
		return TT_CHR;
	else if (S_ISBLK(mode))
		return TT_BLK;
	else if (S_ISWHT(mode))
		return TT_WHT;

	return TT_UNKNOWN;
}

const char *
get_object_type_str(OSTORE_OBJECT_TYPE otype)
{
	for (size_t i = 0; i < type_fields_cnt; ++i) {
		if(type_fields[i].val == otype)
			return type_fields[i].name;
	}

	return NULL;
}

const char *
get_object_symblnk_type_str(OSTORE_SYMBLNK_TYPE ltype)
{
	switch (ltype) {
		case OLT_DIR:
			return TT_CONTAINER.c_str();
		case OLT_FILE:
			return TT_OBJECT.c_str();
		default:
			return TT_UNKNOWN.c_str();
	}
}

/**
 * From the given object info, formulate its container path
 */
void
get_container_path(list_object_context *ctx, const iobj_list_obj_info *info,
    std::stringstream &path)
{

	std::list<const iobj_list_obj_info *> stack;
	const iobj_list_obj_info *elem = info;
	int cnt = 0;
	while (elem) {
		stack.push_front(elem);
		elem = elem->container;
		++cnt;
	}

	path << "/";
	if (ctx->store_.get_store_type() != OST_OBJECT) {
		path << ctx->store_.get_name();
		if (ctx->bucket_path_.size() && ctx->bucket_path_[0] != '/')
			path << "/";
	}
	path << ctx->bucket_path_;

	std::list<const iobj_list_obj_info *>::const_iterator it;
	int i = 0;
	for (it = stack.begin(); it != stack.end() && i < cnt - 1; ++it, ++i) {
		elem = *it;
		if (path.str()[path.str().size()-1] != '/')
			path << "/";
		path << elem->name;
	}
}

void
get_cookie(int max_depth, const iobj_list_obj_info *info,
    resume_cookie &cookie)
{
	std::list<const iobj_list_obj_info *> stack;
	const iobj_list_obj_info *elem = info;
	int cnt = 0;
	while (elem) {
		stack.push_front(elem);
		elem = elem->container;
		++cnt;
	}
	cookie.cookies_.reserve(cnt);
	std::list<const iobj_list_obj_info *>::const_iterator it;
	int i = 0;
	for (it = stack.begin(); it != stack.end(); ++it, ++i) {
		elem = *it;
		cookie.cookies_.push_back(elem->cookie);
	}

	// we are stopped in the middle of a directory, we want to
	// drive down to the children next time to resume, so push
	// a dummy token
	if (info->object_type == OOT_CONTAINER &&
	    (max_depth < 0 || info->cur_depth < max_depth))
		cookie.cookies_.push_back(0);
}

/**
 * From the given object info, formulate its url
 */
void
get_url(list_object_context *ctx, const iobj_list_obj_info *info,
    std::stringstream &path)
{

	std::list<const iobj_list_obj_info *> stack;
	const iobj_list_obj_info *elem = info;
	int cnt = 0;
	while (elem) {
		stack.push_front(elem);
		elem = elem->container;
		++cnt;
	}
	path << ctx->input_.get_base_url() << "/";

	if (ctx->store_.get_store_type() != OST_OBJECT) {
		path << ctx->store_.get_name();
		if (ctx->bucket_path_.size() && ctx->bucket_path_[0] != '/')
			path << "/";
	}

	path << ctx->bucket_path_;

	std::list<const iobj_list_obj_info *>::const_iterator it;
	for (it = stack.begin(); it != stack.end(); ++it) {
		elem = *it;
		if (path.str()[path.str().size()-1] != '/')
			path << "/";
		path << elem->name;
	}
}

/**
 * Load system attributes
 * @param[IN] ctx - the callback context
 * @param[OUT] objectVal - the object value JSON object
 * @param[IN] info - object information received from lower level
 */
void
load_sys_attrs(list_object_context *ctx, Json::Value &objectVal,
    const iobj_list_obj_info *info)
{
	if (to_load_sys_attr(ctx, ISI_HIDDEN))
		objectVal[ISI_HIDDEN] = query_util::is_hidden(info->name);

	if (to_load_sys_attr(ctx, ISI_ID) || ctx->query_.to_sort())
		objectVal[ISI_ID] = info->id;

	if (to_load_sys_attr(ctx, ISI_OBJECT_TYPE)) {
		objectVal[ISI_OBJECT_TYPE] =
		    get_object_type_str(info->object_type);

		if (info->object_type == OOT_LNK) {
			objectVal[ISI_SYMBLNK_TYPE] =
			    get_object_symblnk_type_str(info->symblnk_type);
		}
	}

	if (to_load_sys_attr(ctx, ISI_CONTAINER_PATH)) {
		std::stringstream path;
		get_container_path(ctx, info, path);
		objectVal[ISI_CONTAINER_PATH] = path.str();
	}

	if (to_load_sys_attr(ctx, ISI_URL)) {
		std::stringstream url;
		get_url(ctx, info, url);
		objectVal[ISI_URL] = url.str();
	}

	// following fields originate from stat info
	const struct stat *pstat = info->stats;

	if (pstat == NULL || pstat->st_ino == 0)
		return;
	if (to_load_sys_attr(ctx, ISI_STUB)) {
		if ((pstat->st_flags & SF_FILE_STUBBED) != 0) 
			objectVal[ISI_STUB] = true;
		else
			objectVal[ISI_STUB] = false;
	}

	if (to_load_sys_attr(ctx, ISI_SIZE))
		objectVal[ISI_SIZE] = pstat->st_size;

	if (to_load_sys_attr(ctx, ISI_BLK_SIZE))
		objectVal[ISI_BLK_SIZE] = pstat->st_blksize;

	if (to_load_sys_attr(ctx, ISI_BLOCKS))
		objectVal[ISI_BLOCKS] = pstat->st_blocks;

	if (to_load_sys_attr(ctx, ISI_MTIME))
		objectVal[ISI_MTIME] = pstat->st_mtime;

	if (to_load_sys_attr(ctx, ISI_CTIME))
		objectVal[ISI_CTIME] = pstat->st_ctime;

	if (to_load_sys_attr(ctx, ISI_ATIME))
		objectVal[ISI_ATIME] = pstat->st_atime;

	if (to_load_sys_attr(ctx, ISI_BTIME))
		objectVal[ISI_BTIME] = pstat->st_birthtime;

	if (to_load_sys_attr(ctx, ISI_MTIME_VAL))
		objectVal[ISI_MTIME_VAL] = pstat->st_mtime;

	if (to_load_sys_attr(ctx, ISI_CTIME_VAL))
		objectVal[ISI_CTIME_VAL] = pstat->st_ctime;

	if (to_load_sys_attr(ctx, ISI_ATIME_VAL))
		objectVal[ISI_ATIME_VAL] = pstat->st_atime;

	if (to_load_sys_attr(ctx, ISI_BTIME_VAL))
		objectVal[ISI_BTIME_VAL] = pstat->st_birthtime;

	if (to_load_sys_attr(ctx, ISI_OWNER))
		objectVal[ISI_OWNER] = pstat->st_uid;

	if (to_load_sys_attr(ctx, ISI_GROUP))
		objectVal[ISI_GROUP] = pstat->st_gid;

	if (to_load_sys_attr(ctx, ISI_UID))
		objectVal[ISI_UID] = pstat->st_uid;

	if (to_load_sys_attr(ctx, ISI_GID))
		objectVal[ISI_GID] = pstat->st_gid;


	if (to_load_sys_attr(ctx, ISI_NLINK))
		objectVal[ISI_NLINK] = pstat->st_nlink;

	if (to_load_sys_attr(ctx, ISI_ACC_MODE))
		objectVal[ISI_ACC_MODE] =
		    query_util::get_mode_octal_str(pstat);
}

/**
 * List object callback function
 * @param[IN] context - the callback context pointer
 * @param[IN] info - the object info
 */
bool
list_object_callback(void *context, const iobj_list_obj_info *info)
{
	bool cont = true;
	ASSERT((context != NULL && info != NULL && info->name != NULL));
	const iobj_object_attrs *attr_list = info->attr_list;

	list_object_context *ctx = (list_object_context*) context;
	// if reached maximum of keys than the user desired, break
	if (ctx->keys_written_ == ctx->query_.get_max_keys()) {
		ctx->has_more_ = true;
		return false;
	}
	do {
		++ctx->traversed_cnt_;
		// when either stat or uda are requested, the operation becomes
		// much more expensive, apply restriction
		if (ctx->query_.to_sort() &&
		    (ctx->query_.load_stat() || ctx->query_.load_uda()) &&
		    ctx->traversed_cnt_ > oapi_config::get_max_sort_dir_sz()) {
			cont = false;
			ctx->exceeded_limit_ = true;
			break;
		}

		jdt_object *t_obj = new jdt_object(idt_class::instance());

		Json::Value &objectVal = t_obj->get_json();
		objectVal[ISI_NAME] = Json::Value(info->name);
		objectVal[ISI_BUCKET] = ctx->bucket_;

		load_sys_attrs(ctx, objectVal, info);

		std::string val;
		for (int i = 0; attr_list && i < attr_list->num_entries; ++i) {
			const iobj_obj_attr_entry &entry =
			    attr_list->attrs[i];

			val.assign((const char*)entry.value, entry.value_size);

			std::string key = entry.key;

			if (key == ATTR_CONTENT_TYPE)
				objectVal[ISI_CONTENT_TYPE] = val;
			else
				objectVal[EATTR_PREFIX + key] = val;
		}

		if (!ctx->query_.rule().evaluate(*t_obj)) {
			delete t_obj;
			continue;
		}

		Json::Value::Members members = objectVal.getMemberNames();
		// change the following to string for sorting purpose
		for (size_t i = 0; i < members.size(); ++i) {
			std::string &mem_name = members[i];
			if (query_util::is_owner_attr(mem_name)) {
				uid_t uid = objectVal[mem_name].asUInt();
				set_user_name_from_uid(ctx->user_loader_,
				    objectVal, mem_name, uid);
			}
			else if (query_util::is_group_attr(mem_name)) {
				gid_t gid = objectVal[mem_name].asUInt();
				set_group_name_from_gid(ctx->group_loader_,
				    objectVal, mem_name, gid);
			}
		}

		// add to the sort set.
		if (ctx->query_.to_sort() && ctx->file_set_) {
			ctx->file_set_->insert(t_obj);
			continue;
		}

		write_object_list_entry(objectVal, ctx);
		delete t_obj;

	} while (false);

	if (cont && ctx->keys_written_ == ctx->query_.get_max_keys()) {
		// set the resume token.
		if (!ctx->query_.to_sort()) {
			jdt_object dummy_key(idt_class::instance());
			resume_cookie cookie;
			get_cookie(ctx->max_depth_, info, cookie);
			ctx->token_ << cookie << dummy_key;
		}
	}

	return cont;
}

/**
 * List store callback function
 * @param[IN] context - the callback context pointer
 * @param[IN] info - the object info
 */
static bool
list_store_callback(void *context, const iobj_list_obj_info *info)
{
	bool cont = true;
	ASSERT((context != NULL && info != NULL && info->name != NULL));

	list_base_context *ctx = (list_base_context*) context;
	if (ctx->keys_written_ == ctx->query_.get_max_keys()) {
		ctx->has_more_ = true;
		return false;
	}
	do {
		++ctx->traversed_cnt_;
		// when either stat or uda are requested, the operation becomes
		// much more expensive, apply restriction
		if (ctx->query_.to_sort() &&
		    (ctx->query_.load_stat() || ctx->query_.load_uda()) &&
		    ctx->traversed_cnt_ > oapi_config::get_max_sort_dir_sz()) {
			cont = false;
			ctx->exceeded_limit_ = true;
			break;
		}

		jdt_object *t_obj = new jdt_object(idt_class::instance());

		Json::Value &objectVal = t_obj->get_json();
		objectVal[ISI_NAME] = Json::Value(info->name);
		objectVal[ISI_PATH] = Json::Value(info->path);
		if (!ctx->query_.rule().evaluate(*t_obj)) {
			delete t_obj;
			continue;
		}
		// add to the sort set.
		if (ctx->query_.to_sort() && ctx->file_set_) {
			ctx->file_set_->insert(t_obj);
			continue;
		}

		write_ns_store_list_entry(objectVal, ctx);
		delete t_obj;

	} while (false);

	// if reached maximum of keys than the user desired, break
	if (cont && ctx->keys_written_ == ctx->query_.get_max_keys()) {
		// set the resume token.
		if (!ctx->query_.to_sort()) {
			jdt_object dummy_key(idt_class::instance());
			resume_cookie cookie;
			get_cookie(0, info, cookie);
			ctx->token_ << cookie << dummy_key;
		}
	}

	return cont;
}

/**
 * List the the objects under the bucket.
 * @param[IN] bucket - the bucket
 * @param[IN] param - the search parameter
 * @param[OUT] error - the error on failure
 */
void
list_object(oapi_bucket &bucket,
    iobj_list_obj_param & param, isi_error *&error)
{
	param.ibh = bucket.get_handle();
	iobj_object_list(&param, &error);
	if (error)
		isi_error_add_context(error);
}

struct list_bucket_context {
	oapi_store &store_;
	list_object_context &lo_context_;
	bool load_stat_;
	bool load_uda_;
};

static bool
list_bucket_callback(void *context, const char *name)
{
	struct isi_error *error = NULL;

	ASSERT(context && name);

	list_bucket_context *ctx = (list_bucket_context*) context;

	jdt_object b_obj(dtc_bucket::instance());
	b_obj.set_attr(ISI_BUCKET, name);

	// if the bucket does not satisfy the conditions
	// no need to check the objects under
	if (!ctx->lo_context_.query_.rule().evaluate(b_obj))
		return true;

	list_obj_param t_param;
	iobj_list_obj_param &param = t_param.param_;
	param.caller_cb = list_object_callback;
	param.stats_requested = ctx->load_stat_;
	param.uda_requested = ctx->load_uda_;
	param.caller_context = &ctx->lo_context_;

	ctx->lo_context_.bucket_ = name; // set the bucket name
	ctx->lo_context_.bucket_path_ = name; // same as the above in object

	oapi_bucket bucket(ctx->lo_context_.store_);

	bucket.open(SH_ACCT, name, 
	    (enum ifs_ace_rights) BUCKET_READ, 0,
	    CF_FLAGS_NONE, error);

	if (error) {
		oapi_error_handler().throw_on_error(error,
		    "open a bucket");
	}

	list_object(bucket, param, error);

	if (error) {
		oapi_error_handler().throw_on_error(error,
		    "list a bucket");
	}

	return true;
}

/**
 * This routine list all the objects under all buckets.
 * Used only for two-tier object store.
 */
void
list_all_object(
    oapi_store &store,
    iobj_list_obj_param &param,
    list_object_context &lo_context,
    isi_error *&error)
{
	std::string user;
	lo_context.input_.get_header(HDR_REMOTE_USER, user);
	oapi_acct acct(user);

	list_bucket_context context = {store, lo_context,
	    param.stats_requested, param.uda_requested};

	acct.list_bucket(store, list_bucket_callback, &context, error);

	if (error)
		isi_error_add_context(error);
}

/**
 * Check the validity of the system attribute name. If not valid, throw
 * API exception
 * @param[IN] attr_magr - the attribute manager
 * @param[IN] attr - the attribute
 * @throw api_exception when the attribute is invalid
 */
void
check_sys_attribute(const attribute_manager &attr_mgr, const std::string &attr)
{
	if (!attr_mgr.is_valid_attr(attr)) {
		throw api_exception(AEC_BAD_REQUEST,
		    "The attribute '%s' is not a valid system attribute.",
		    attr.c_str());
	}
}

/**
 * select the default detailed attributes
 * @param[IN] attr_magr - the attribute manager
 * @param[IN/OUT] query - the query object
 */
void
select_default_attrs(const attribute_manager &attr_mgr, object_query &query)
{
	std::list<std::string> deflt_attrs;

	attr_mgr.get_default(deflt_attrs);
	std::list<std::string>::iterator it;
	for (it = deflt_attrs.begin(); it != deflt_attrs.end(); ++it)
		query.select_attr(*it);
}

/**
 * select the detailed attributes from the request
 * @param[IN] detail_str - the detail request string
 * @param[IN] attr_magr - the attribute manager
 * @param[IN/OUT] query - the query object
 * @throw api_exception when an attribute requested in details is invalid
 */
bool
select_detail_attrs(const std::string &detail_str,
    const attribute_manager &attr_mgr, object_query &query)
{
	static const char *detail_default = "default";
	if (detail_str.empty()) {
		return false;
	}

	bool show_default_sys_attr = (detail_str == "yes") ||
	    (detail_str == "y") ||
	    (detail_str == "Y") ||
	    !strcasecmp(detail_str.c_str(), detail_default);

	if (!show_default_sys_attr) {
		// this a list of system attributes.
		// add them

		std::vector<std::string> fields = request::tokenize(
		    detail_str, ",", false);

		for (size_t i = 0; i < fields.size(); ++i) {

			const std::string &field = fields[i];
			// verify the attribute selected
			// make sure it is valid
			if (!strcasecmp(field.c_str(),
			    detail_default)) {
				select_default_attrs(attr_mgr, query);
				continue;
			}

			if (query_util::is_sys_attr(field))
				check_sys_attribute(attr_mgr, field);
			query.select_attr(field);
		}
	}
	else
		select_default_attrs(attr_mgr, query);

	return true;
}

/**
 * Figure out the sort information from the sort argument
 * @param[IN] arg_sort - the sort argument
 * @param[IN] arg_dir - the sort direction
 * @param[IN] attr_magr - the attribute manager
 * @param[IN/OUT] query - the query object
 * @throw api_exception when an attribute requested in details is invalid
 */
void
figure_sort_info(const std::string &arg_sort, sort_dir arg_dir,
    const attribute_manager &attr_mgr,
    object_query &query)
{
	if (arg_sort.empty())
		return;

	// the sort fields are comma separated list of system attributes
	std::vector<std::string> fields = request::tokenize(arg_sort,
	    ",", false);
	sort_field field;
	for (size_t i = 0; i < fields.size(); ++i) {
		const std::string &attr = fields[i];
		if (query_util::is_sys_attr(attr))
			check_sys_attribute(attr_mgr, attr);

		field.dir_ = (sort_dir)arg_dir;
		field.field_ = attr;
		query.add_sort_field(field);
	}
}

/**
 * Given the input, initialize the JSON query.
 * @param[IN] input - the request
 * @param[OUT] query - the query
 * @throw api_exception in case of invalid query.
 */
void
initialize_query(const request &input, object_query &query)
{
	if (input.content_length() > 0) {
		if (input.content_length() > MAX_QUERY_SIZE) {
			throw api_exception(AEC_BAD_REQUEST,
			    "The query size '%zu' exceeded the limit: '%d'.",
			    input.content_length(), MAX_QUERY_SIZE);
		}
		const Json::Value & val = input.get_json();

		if (!val.isObject()) {
			throw api_exception(AEC_BAD_REQUEST,
			    "Invalid JSON query. It must be a JSON Object. "
			    "Please refer to the API documentation.");
		}
		query.initialize(val);
	}
	else
		query.initialize(Json::Value::null);
}

/**
 * Figure out the list parameters from the input and the query.
 * @param[OUT] param - the list object parameter
 * @param[IN] callback - the list object callback function
 * @param[IN] input - the request
 * @param[IN/OUT] query - the query object
 * @param[IN/OUT] context - the list object callback context object
 */
void
init_query_param(iobj_list_obj_param &param, iobj_list_obj_cb callback,
    const request &input, object_query &query,
    list_base_context &context)
{
	const attribute_manager &attr_mgr = query.get_attr_mgr();

	std::string max_keys_str, detail_str, resume;
	bool            type_set       = false;
	int             arg_type       = OOT_ALL;
	bool		hidden_set     = false;
	bool            arg_hidden     = false;
	std::string     arg_sort;
	int             arg_dir        = SORT_DIR_ASC;
	unsigned	arg_limit      = DEFAULT_MAX_KEYS;
	resume_cookie 	resume_cookie; // used for unsorted operation
	const request::arg_map &args = input.get_arg_map();
	int		arg_depth      = 0;
	bool		resuming       = false;

	if (input.get_arg(PRM_RESUME, resume)) {
		resume_input_token rit(resume, GET_TOKEN_VERSION);
		rit >> arg_type >> arg_hidden >> arg_sort >> arg_dir
		    >> arg_limit >> arg_depth >> detail_str >> resume_cookie
		    >> context.resume_key_;
		resuming = true;
	}
	else {
		type_set = find_args(arg_type,
		    arg_to_type_field, args, PRM_TYPE);

		hidden_set = find_args(arg_hidden, arg_to_bool(), args,
		    PRM_HIDDEN);

		input.get_arg(PRM_SORT, arg_sort);
		find_args(arg_dir, arg_to_sort_dir(), args, PRM_DIR);

		if (input.get_arg(PRM_MAX_KEYS, max_keys_str)) {
			char *endptr = NULL;
			uint64_t mk = strtoull(max_keys_str.c_str(),
			    &endptr, 10);
			if (!(mk == 0 && errno == EINVAL))
				arg_limit = mk;
		}
		input.get_arg(PRM_DETAIL, detail_str);
		find_args(arg_depth, arg_to_generic<int>(), args, PRM_DEPTH);
	}

	select_detail_attrs(detail_str, attr_mgr, query);
	figure_sort_info(arg_sort, (sort_dir)arg_dir, attr_mgr, query);

	// preset the invariable token part:
	// the cookie and the key will be set at post process
	context.token_ << arg_type << arg_hidden << arg_sort << arg_dir
	    << arg_limit  << arg_depth << detail_str;

	query.set_max_keys(arg_limit);

	file_ptr_set *fset = NULL;
	if (query.to_sort()) {
		if (arg_limit > MAX_SORT_LIMIT ||
		    arg_limit <= 0) {
			throw api_exception(AEC_BAD_REQUEST,
			    "The sort limit '%u' exceeded the limits: '0-%d'.",
			    arg_limit, MAX_SORT_LIMIT);
		}

		fset = new file_ptr_set(arg_limit + 1,
		    resuming ? &context.resume_key_ : NULL,
		    file_sorter(query.get_sort_info()));
	}
	context.file_set_ = fset;
	context.attr_mgr_ = &attr_mgr;

	if (type_set && arg_type != OOT_ALL) {
		const char *type_name = get_object_type_str(
		    (OSTORE_OBJECT_TYPE)arg_type);
		query.set_load_object_type(type_name);
	}
	if (hidden_set)
		query.set_load_hidden(arg_hidden);

	// compile the JSON requests:
	initialize_query(input, query);

	param.caller_cb = callback;
	param.stats_requested = query.load_stat();
	param.uda_requested = query.load_uda();
	param.cookie_count = resume_cookie.cookies_.size();
	if (param.cookie_count > 0) {
		param.resume_cookie = new uint64_t[param.cookie_count];

		memcpy(param.resume_cookie, &resume_cookie.cookies_.at(0),
		    sizeof(uint64_t) * resume_cookie.cookies_.size());
	}

	param.max_depth = arg_depth;
}

/**
 * Common routine to post-process a search results
 */
void
post_process_search(isi_error *error, list_base_context &context,
    response &output)
{
	if (error) {
		oapi_error_handler().throw_on_error(error,
		    "list bucket or object");
	}

	if (context.had_error_) {
		ilog(IL_DEBUG, "Had error writing to the client");
		return;
	}

	if (context.exceeded_limit_) {
		throw api_exception(AEC_LIMIT_EXCEEDED,
		    "The container has too many children to support"
		    " sorting. Traversed count: %zu, "
		    " supported limit: %zu",
		    context.traversed_cnt_,
		    oapi_config::get_max_sort_dir_sz());
	}

	// if no entries found, write an empty array.
	if (context.keys_written_ == 0 && (!context.query_.to_sort() ||
	    !context.file_set_ || context.file_set_->size() == 0)) {
		Json::Value val; //write empty once for output array heading
		context.writer_(val, &context);
		goto closing; // for array ending
	}

	// write out the entries:
	if (context.file_set_) {
		context.file_set_->finalize(); // sort it first
		file_ptr_set::const_iterator it = context.file_set_->begin(),
		    end = context.file_set_->end();
		Json::Value val;
		for (size_t i = 0; it != end &&
		    i < context.query_.get_max_keys();
		    ++it, ++i)
		{
			const jdt_object *obj = *it;
			val = obj->get_json();
			context.writer_(val, &context);
		}

		resume_cookie cookie;
		context.token_ << cookie;
		if (it != end) {
			// Only fields used in the sorting need to be
			// serialized to reduce the token size.
			jdt_object *obj = *(it-1);
			Json::Value &tval = obj->get_json();
			Json::Value::Members members = tval.getMemberNames();

			for (size_t i = 0; i < members.size(); ++i) {
				std::string &mem_name = members[i];
				if (!context.query_.is_attr_sorted(mem_name) &&
				    mem_name != ISI_ID) {
					tval.removeMember(mem_name);
					continue;
				}
			}

			context.token_<< tval;
			context.has_more_ = true;
		}
	}
closing:
	output.write_data("]", sizeof("]") - 1);

	// write the the resume token:
	if (context.has_more_) {
		ASSERT(context.keys_written_ == context.query_.get_max_keys());
		output.write_data(",", sizeof(",") - 1);
		const std::string &token = context.token_.str();
		static const std::string resume("\"resume\":\"");
		output.write_data(resume.c_str(), resume.size());
		output.write_data(token.c_str(), token.size());
		output.write_data("\"", sizeof("\"") - 1);
	}

	// write the closing bracket:
	output.write_data("}", sizeof("}") - 1);
}

}

// The following three functions are not utilized yet:
/*
 * Function to convert from GID to group name
 */

void
group_name_from_id(const Json::Value &src, Json::Value &dest)
{

}

/*
 * Function to convert from UID to user name
 */
void
user_name_from_id(const Json::Value &src, Json::Value &dest)
{

}

/*
 * Function to convert from time_t to HTTP date string
 */
void
date_str_from_val(const Json::Value &src, Json::Value &dest)
{

}

void
query_handler::list_store(enum OSTORE_TYPE store_type,
    const request &input, response &output)
{
	list_obj_param param;
	object_query query(store_provider::get_attribute_mgr(input));

	isi_error *error = NULL;
	std::string user;
	input.get_header(HDR_REMOTE_USER, user);

	list_base_context context(input, query, output,
	    write_ns_store_list_entry);

	init_query_param(param.param_, list_store_callback, input,
	    query, context);

	param.param_.caller_context = &context;

	iobj_ostore_list(store_type, user.c_str(), &param.param_, &error);
	if (error)
		isi_error_add_context(error);
	post_process_search(error, context, output);
}

void
query_handler::search_object(oapi_bucket &bucket,
    const request &input, response &output)
{
	list_obj_param param;
	object_query query(store_provider::get_attribute_mgr(input));

	list_object_context context(input, query, output,
	    bucket.get_name(), bucket.get_path(), bucket.get_store());

	init_query_param(param.param_, list_object_callback, input,
	    query, context);
	param.param_.caller_context = &context;
	context.max_depth_ = param.param_.max_depth;

	isi_error *error = NULL;
	list_object(bucket, param.param_, error);
	post_process_search(error, context, output);
}

void
query_handler::search_object(const std::string &bucket_name,
    const request &input, response &output)
{
	oapi_store store;
	store_provider::open_store(store, input);

	isi_error *error = NULL;

	ASSERT(bucket_name.size() > 0, "Bucket name must be set.");

	oapi_bucket bucket(store);

	bucket.open(SH_ACCT, bucket_name.c_str(), 
	    (enum ifs_ace_rights) BUCKET_READ, 0, 
	    CF_FLAGS_NONE, error);

	if (error) {
		oapi_error_handler().throw_on_error(error,
		    "open a bucket");
	}

	search_object(bucket, input, output);
}

void
query_handler::search_object(const request &input, response &output)
{
	oapi_store store;
	store_provider::open_store(store, input);

	object_query query(store_provider::get_attribute_mgr(input));

	list_obj_param param;
	list_object_context context(input, query, output, "", "", store);

	init_query_param(param.param_, list_object_callback, input,
	    query, context);
	param.param_.caller_context = &context;
	context.max_depth_ = param.param_.max_depth;

	isi_error *error = NULL;
	// list buckets and then list object recursively
	list_all_object(
	    store,
	    param.param_,
	    context,
	    error);

	post_process_search(error, context, output);
}

