#ifndef __CPOOL_STAT_SHARED_MAP_H__
#define __CPOOL_STAT_SHARED_MAP_H__

#include <stdio.h>
#include <vector>
#include <map>
#include <stdexcept>
#include <string>
#include <errno.h>
#include <math.h>
#include <pthread.h>

#include <sys/shm.h>
#include <sys/sem.h>

#include <isi_util/isi_assert.h>
#include <isi_util/util.h>
#include <isi_util/isi_error.h>
#include <isi_util_cpp/isi_exception.h>
#include <isi_util_cpp/scoped_lock.h>
#include <isi_ilog/ilog.h>

#include "cpool_stat_list_element.h"
#include "cpool_stat_read_write_lock.h"

#define CP_STAT_SHARED_MAP_PAGE_SIZE 50

#define MAX_SHMEM_NAME_LENGTH 4096
#define CP_STAT_SHMEM_PATH "/tmp/cpool_stat_"
#define CP_STAT_FTOK_PROJ_ID 8675309

/**
 * A template implementation of a map leveraging shared memory.
 *通过get_instance返回cpool_stat_shared_map
 * - Consumers may not create an instance of this class directly.  Instead
 *   consumers must call the static member function 'get_instance' to
 *   return an instance of cpool_stat_shared_map.
 *浅拷贝<amount_key_t, cpool_stat>
 * - This class assumes shallow values and keys; that is, shallow copies of
 *   keys and values are made at assignment time (e.g., by
 *   cpool_stat_list_element), so embedded pointers will not function correctly.
 *
 * 由于是共享内存,删除instance不会free shared memory.因此把空间给其他进程使用
 * - Because of the shared memory nature of this class, deleting an instance
 *   will not free shared memory, but will leave it available for other
 *   processes to consume
 *页机制:当一个页中50个数据满了之后,动态创建另外一个instance(包含一个页),类似于链表通过指针连接(一个stat_shared_map链接另外一个stat_shared_map)
 * - A paging mechanism is used to allow shared memory to grow dynamically as
 *   needed.  As this "page" (e.g., cpool_stat_shared_map instance) fills up,
 *   a new instance is created and referenced by this instance similar to
 *   linked list implementations
 */
template <class map_key_class, class value_class> //////<account_key, cpool_stat>
class cpool_stat_shared_map
{
public:

	/**
	 * Destructor - releases resources used by this instance.  Does not
	 * free shared memory, but does release pointer to shared memory
	 */
	virtual ~cpool_stat_shared_map();

	/**
	 * Returns pointer to the value referenced by the given key.  If the
	 * given key does not exist, the error is populated with an ENOENT
	 * system error and NULL is returned.
	 * Note* in order to be used safely, caller should call 'lock_map'
	 * before calling 'at' and 'unlock_map' after modifications are
	 * complete.  This will guarantee that the returned pointer remains
	 * valid while being used.
	 *
	 * @param key - a key indicating which value should be returned.  If
	 *              key does not exist in map, error_out isi populated and
	 *              NULL will be returned
	 * @param error_out - standard Isilon error parameter.  Will be
	 *                    populated with an ENOENT system error if the key
	 *                    is not found or by errors pertaining to locking or
	 *                    shared memory loading
	 * @return - a pointer to the value corresponding to the given key.  If
	 *           the key does not exist, error_out will be populated and
	 *           NULL will be returned
	 */
	value_class *at(const map_key_class &key, struct isi_error **error_out);

	/**
	 * Inserts the given key and value into the map if the key does not
	 * already exist.  If the key does exist, no change is made and false
	 * is returned
	 *
	 * @param key - a key to refer to the new value in the map.  If the key
	 *              already exists in the map, no change will be made
	 * @param value - the new value to be added to the map
	 * @param error_out - standard Isilon error parameter.  Could be EEXIST
	 *                    system error if given key already exists in map.
	 *                    If not, will only be populated from errors
	 *                    pertaining to locking or shared memory loading
	 */
	void insert(const map_key_class &key, const value_class &value,
	    struct isi_error **error_out);

	/**
	 * Removes the value referred to by the given key.  If the key does not
	 * exist in the map, no change is made and false is returned
	 *
	 * @param key - a key indicating which value should be removed.  If key
	 *              does not exist in the map, no change will be made
	 * @param error_out - standard Isilon error parameter.  Will only be
	 *                    populated from errors pertaining to locking
	 * @return - a boolean indicating whether a change was successfully
	 *           made or not; that is, whether the given key was found
	 *           in the map or not.
	 */
	bool erase(const map_key_class &key, struct isi_error **error_out);

	/**
	 * Removes every value from the map.  Does *not* attempt to shrink the
	 * map by freeing extended shared memory pages
	 *
	 * @param error_out - standard Isilon error parameter.  Will only be
	 *                    populated from errors pertaining to locking
	 */
	void clear(struct isi_error **error_out);

	/**
	 * Returns the number of elements in the map
	 *
	 * @param error_out - standard Isilon error parameter.  Will only be
	 *                    populated from errors pertaining to locking
	 * @return - the number of elements in the map
	 */
	size_t size(struct isi_error **error_out);

	/**
	 * Returns a pointer to the value referenced by the given key.  If the
	 * key does not exist in the map, a NULL pointer is returned
	 *
	 * @param key - the key to search for
	 * @param error_out - standard Isilon error parameter.  Will only be
	 *                    populated from errors pertaining to locking
	 * @return - a pointer to the value referenced by key if it exists.
	 *           NULL if the key does not exist
	 */
	const value_class *find(const map_key_class &key,
	    struct isi_error **error_out);

	/**
	 * Locks the entire map for performing multiple read and write
	 * operations.  Must be unlocked by calling unlock_map.  Cannot be
	 * called again until unlocked.
	 *
	 * @param error_out - standard Isilon error parameter
	 */
	void lock_map(struct isi_error **error_out);

	/**
	 * Unlocks the entire map after a call to lock_map.
	 *
	 * @param error_out - standard Isilon error parameter
	 */
	void unlock_map(struct isi_error **error_out);

	/**
	 * Populates given cpool_stat_list_element vector with all (non-empty)
	 * members of this map.
	 * Note* in order to be used safely, caller should call 'lock_map'
	 * before calling 'as_vector' and 'unlock_map' after modifications are
	 * complete.  This will guarantee that the returned pointers remain
	 * valid while being used.
	 *
	 * @param out_vec - a vector buffer to hold all elements of this map
	 */
	void as_vector(
	    std::vector<cpool_stat_list_element<map_key_class, value_class>*>
		&out_vec, struct isi_error **error_out);

	/**
	 * Populates given map_key_class vector with all (non-empty)
	 * keys in this map.
	 * Note* in order to be used safely, caller should call 'lock_map'
	 * before calling 'as_vector' and 'unlock_map' after modifications are
	 * complete.  This will guarantee that the returned pointers remain
	 * valid while being used.
	 *
	 * @param out_vec - a vector buffer to hold all keys of this map
	 */
	void keys_as_vector(std::vector<const map_key_class*> &out_vec,
	    struct isi_error **error_out);

	/**
	 * A static function for returning a named instance of this shared map
	 * with the given key and value class types.  The instance will
	 * reference the shared memory with the given name.  Instances are
	 * quasi-singletons; that is, multiple instance will exist, but for any
	 * given name there will be exactly one instance created.
	 *
	 * @tparam map_key_class - the class type of keys in the new map (NOTE*
	 *                     key types should not hold pointers to ensure
	 *                     correct behavior)
	 * @tparam value_class - the class type of values in the new map (NOTE*
	 *                       value types should not hold pointers to ensure
	 *                       correct behavior)
	 * @param name - an arbitrary name for the shared map being  referenced.
	 *               If this same segment will be leveraged by another
	 *               process (or thread), the other process must also use
	 *               the same name.  Name must follow filename rules for
	 *               linux
	 * @param error_out - standard Isilon error parameter.
	 * @return - a pointer to a mounted instance of a cpool_stat_shared_map.
	 */
	static cpool_stat_shared_map<map_key_class, value_class>*
	    get_instance(const char *name, struct isi_error **error_out);

	/*
	 * For use in test suites only
	 */
	void reset_after_test(struct isi_error **error_out);

	/**
	 * For testing purposes
	 * Preallocates storage space for the given number of accounts to avoid
	 * false positive memory leaks in shared memory
	 *
	 * @param num_potential_accounts  The number of accounts which should
	 *                                have available stats space
	 */
	void preallocate_space_for_tests(int num_potential_accounts,
	    struct isi_error **error_out);

private:
	/**
	 * Don't want any direct creation so we can manage memory manually and
	 * return isi_errors when allocation succeeds, but shared memory mount
	 * fails
	 */
	cpool_stat_shared_map(const char *lock_name) : next_(NULL),
	    shared_mem_(NULL), lock_(lock_name), map_locked_(false)
	{
		pthread_mutex_init(&mutex_, NULL);
	};

	/**
	 * A static function for creating an instance of this shared map with
	 * the given key and value class types.  The new instance will
	 * reference the shared memory with the given name.  If shared memory
	 * cannot be mounted, error_out is populated.  Consumer is responsible
	 * for freeing the new instance when done
	 *
	 * @tparam map_key_class - the class type of keys in the new map (NOTE*
	 *                     key types should not hold pointers to ensure
	 *                     correct behavior)
	 * @tparam value_class - the class type of values in the new map (NOTE*
	 *                       value types should not hold pointers to ensure
	 *                       correct behavior)
	 * @param name - an arbitrary name for the shared memory segment to be
	 *               referenced.  If this same segment will be leveraged by
	 *               another process, the other process must also use the
	 *               same name.  Name must follow filename rules for linux
	 * @param error_out - standard Isilon error parameter.  Will only be
	 *                    populated from errors pertaining to locking or
	 *                    shared memory loading
	 * @return - a newly allocated and mounted instance of a
	 *           cpool_stat_shared_map.  Consumer is resonsible for deleting
	 *           the new instance when done
	 */
	static cpool_stat_shared_map<map_key_class, value_class>*
	    get_new_instance(const char *name, struct isi_error **error_out);

	/**
	 * Finds the index to given key in this instance (does not recurse
	 * through next_ pointers)
	 */
	int find_index(const map_key_class &key) const;

	/**
	 * Lock utilities
	 */
	inline void lock_for_read(struct isi_error **error_out) const
	{
		/* If the entire map is already locked, don't re-lock */
		if (!map_locked_)
			lock_.lock_for_read(error_out);
	};
	inline void lock_for_write(struct isi_error **error_out) const
	{
		/* If the entire map is already locked, don't re-lock */
		if (!map_locked_)
			lock_.lock_for_write(error_out);
	};
	inline void unlock_for_read(struct isi_error **error_out) const
	{
		if (!map_locked_)
			lock_.unlock(error_out);
	};
	inline void unlock_for_write(struct isi_error **error_out) const
	{
		if (!map_locked_)
			lock_.unlock(error_out);
	};

	/**
	 * Similar to an internal constructor, but with an error_out param.
	 * Mounts shared memory names shmem_name and initializes all values.
	 */
	void initialize(const char *shmem_name, struct isi_error **error_out);

	/**
	 * Internal implementation of get_new_instance.  Has a parameter for
	 * enabling/disabling locks (e.g., to avoid double locking)
	 */
	static cpool_stat_shared_map<map_key_class, value_class>*
	    get_new_instance_impl(const char *name, bool take_lock,
	    struct isi_error **error_out);

	/**
	 * Determines appropriate name for next linked shared memory "page",
	 * creates an instance and references it from the next_ pointer
	 */
	void mount_next(struct isi_error **error_out);

	/**
	 * Checks that next_ pointer is populated correctly.  I.e., updates
	 * next_ when it has been changed by another process.
	 */
	void check_next_mounted(struct isi_error **error_out);

	/**
	 * Creates/fetches a shared memory instance and returns a pointer to it
	 */
	static void *create_shared_memory(key_t shmem_key, size_t shmem_size,
	    struct isi_error **error_out);

	/**
	 * Internal implementations of public functions.  No locking at this
	 * level.
	 */
	value_class *at_impl(const map_key_class &key);
	bool insert_impl(const map_key_class &key, const value_class &value);
	bool erase_impl(const map_key_class &key);
	void clear_impl();
	size_t size_impl() const;
	const value_class *find_impl(const map_key_class &key);
	void as_vector_impl(
	    std::vector<cpool_stat_list_element<map_key_class, value_class>*>
		&out_vec);
	void keys_as_vector_impl(std::vector<const map_key_class*> &out_vec);

	/**
	 * A pointer to the next cpool_stat_shared_map "page" (similar to
	 * linked list implementations)
	 */
	////第二页也是一个cpool_stat_shared_map,相当于shared_map连接shared_map
	cpool_stat_shared_map<map_key_class, value_class> *next_;

	/**
	 * Private structure encapsulating the current shared map "page" (e.g.,
	 * array of list elements), the name of the shared memory and whether
	 * there is a next page.  When shared memory is created, it will be
	 * cast to an instance of this struct
	 */
	struct shared {
		cpool_stat_list_element<map_key_class, value_class>
		    list_els_[CP_STAT_SHARED_MAP_PAGE_SIZE];
		char name_[MAX_SHMEM_NAME_LENGTH];
		bool has_next_;  ///是否有下一页
	};

	/**
	 * A pointer to the shared memory structure associated with this map
	 * instance
	 */
	struct shared *shared_mem_;

	/**
	 * A shared lock for controlling reads/writes to this map
	 */
	mutable cpool_stat_read_write_lock lock_;

	bool map_locked_;

	pthread_mutex_t mutex_;
};

/**
 * I M P L E M E N T A T I O N
 * ---------------------------
 * The nature of template classes is such that the implementation is best
 * placed in the header file
 * cpool_stat_shared_map implementation follows
 */

#define ERROR_CREATE_SHARED_MEMORY "Failed to create shared memory"
#define ERROR_GET_SHARED_MEMORY_PTR "Failed to get pointer to shared memory"
#define ERROR_CANNOT_LOCK "Cannot lock map"
#define ERROR_NOT_LOCKED "Map cannot be unlocked without a previous lock call"
#define ERROR_NO_ENTRY_FOUND "No matching entry found in shared map"
#define ERROR_KEY_EXISTS "Specified key already exists in shared map"

// TODO:: Investigate leveraging boost::interprocess shared memory allocator as
// a substitute for this class - see
// http://stackoverflow.com/questions/12413034/shared-map-with-boostinterprocess

// TODO:: Investigate maintaining a key-to-index map internally to speed up
//    lookups

template <class map_key_class, class value_class>
cpool_stat_shared_map<map_key_class, value_class>::~cpool_stat_shared_map()
{
	if (next_ != NULL) {
		delete next_;  /////递归调用析构
		next_ = NULL;
	}

	if (shared_mem_ != NULL)
		shmdt(shared_mem_);

	pthread_mutex_destroy(&mutex_);
}

template <class map_key_class, class value_class>
void*
cpool_stat_shared_map<map_key_class, value_class>::create_shared_memory(
    key_t shmem_key, size_t shmem_size, struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	void *shared_mem_ptr = NULL;
	int shared_mem_id = -1;
	int shmem_permission = 0666;

	/* Create memory and give read/write permissions to world */
	int shared_mem_flags = IPC_CREAT | shmem_permission;

	shared_mem_id = shmget(shmem_key/*ftok(cpool_stat_name)*/, shmem_size/*sizeof(struct shared)*/, shared_mem_flags);

	/* If there was an error creating shared memory, bail out */
	if (shared_mem_id == -1) {
		error = isi_system_error_new(errno, ERROR_CREATE_SHARED_MEMORY);
		goto out;
	}

	/* Get the shared memory pointer */
	////return (struct shared*)shared_mem_ptr;
	shared_mem_ptr = shmat(shared_mem_id, NULL, 0);//shared memory attach
	if (shared_mem_ptr == (void*)(-1)) {
		shared_mem_ptr = NULL;

		/*
		 * Note*
		 * Only a small number (10?) of references to a shared memory
		 * segment are allowed on the system.  If we try to make too
		 * many connections, errno will be set to EMFILE.  For
		 * cpool_stat usage, we expect only 4 or 5 connections, so it
		 * shouldn't be a problem, but if that changes, the max number
		 * of connections can supposedly be modified by mucking with
		 * /etc/system.  E.g., add: set shmsys:shminfo_shmseg = <value>
		 * See https://groups.google.com/forum/#!topic/comp.unix.programmer/EivM3YbL3o8
		 */
		error = isi_system_error_new(errno,
		    ERROR_GET_SHARED_MEMORY_PTR);
		goto out;
	}
out:
	isi_error_handle(error, error_out);
	return shared_mem_ptr;
}

template <class map_key_class, class value_class>
cpool_stat_shared_map<map_key_class, value_class>*
cpool_stat_shared_map<map_key_class, value_class>::get_instance( ////在初始化cpool_stat_manager时调用，用来创建cpool_stat_shared_map
    const char *name, struct isi_error **error_out)
{
	////instances中就2个:一个是for test,另一个not for test
	static std::map<std::string,
	    cpool_stat_shared_map<map_key_class, value_class>*> instances; ////instance是一个std::map<string, cpool_stat_shared_map*>
	static pthread_mutex_t map_instance_mutex =
	    PTHREAD_MUTEX_INITIALIZER;

	struct isi_error *error = NULL;
	cpool_stat_shared_map<map_key_class, value_class>* ret_val = NULL;

	scoped_lock sl(map_instance_mutex);

	try {
		ret_val = instances.at(name);

	/* If the named instance did not exist, create it now */
	} catch (const std::out_of_range &ex) {

		ASSERT(ret_val == NULL);

		ret_val = get_new_instance(name, &error);  ////最开始map中没有shared_map,所以创建
		ON_ISI_ERROR_GOTO(out, error);

		ASSERT(ret_val != NULL);

		instances.insert(std::pair<std::string,
		    cpool_stat_shared_map<map_key_class, value_class>*>(
			name, ret_val));
	}

	ASSERT(ret_val != NULL);

out:
	isi_error_handle(error, error_out);
	return ret_val;
}

template <class map_key_class, class value_class>
cpool_stat_shared_map<map_key_class, value_class>*
cpool_stat_shared_map<map_key_class, value_class>::get_new_instance(
    const char *shmem_name, struct isi_error **error_out)
{
	const bool take_lock = true;
	return get_new_instance_impl(shmem_name, take_lock, error_out);
}

template <class map_key_class, class value_class>
cpool_stat_shared_map<map_key_class, value_class>*
cpool_stat_shared_map<map_key_class, value_class>::get_new_instance_impl(
    const char *shmem_name, bool take_lock, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	cpool_stat_shared_map<map_key_class, value_class> *ret_val =
	    new cpool_stat_shared_map<map_key_class, value_class> (shmem_name);

	ASSERT(ret_val != NULL);

	if (take_lock) {
		ret_val->lock_for_write(&error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	ret_val->initialize(shmem_name, &error);////将每一块cpool_stat_shared_map mount起来(递归): next_ = cpool_stat_map

	if (error != NULL) {
		if (take_lock)
			ret_val->unlock_for_write(
			    isi_error_suppress(IL_NOTICE));

		delete ret_val;
		ret_val = NULL;
		goto out;
	}

	if (take_lock)
		ret_val->unlock_for_write(&error);

out:
	isi_error_handle(error, error_out);

	return ret_val;
}

///////cpool_stat_manager.cpp中#define SHARED_MAP_NAME "cpool_stat_map_"作为shmem_name
///第一块stat_shared_map的名字是"cpool_stat_map_",第二块stat_shared_map的名字是"cpool_stat_map_x"
//第三块....initialize的过程就是像链表一样将这一块一块的stat_shared_map通过next_链接起来
template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::initialize(
    const char *shmem_name, struct isi_error **error_out)
{
	// Don't initialize twice!
	ASSERT(shared_mem_ == NULL);

	struct isi_error *error = NULL;

	key_t shared_mem_key = cpool_stat_name_to_key(shmem_name, &error);
	ON_ISI_ERROR_GOTO(out, error);

	///////要么创建一个新的shared memory,要么就是拿已经存在的shared memory
	shared_mem_ = (struct shared *)
	    create_shared_memory(shared_mem_key, sizeof(struct shared), &error);
	ON_ISI_ERROR_GOTO(out, error);

	/* If name_ isn't equal to shmem_name, we know we haven't
	   initialized the shared memory yet */
	///如果拿到的这块mem的名字不等于shmen_name,说明这块内存以前没被用过
	if (shared_mem_->name_ == NULL ||
	    strncmp(shared_mem_->name_, shmem_name, strlen(shmem_name)) != 0) {

		strcpy(shared_mem_->name_, shmem_name);

		shared_mem_->has_next_ = false;

		next_ = NULL; /////指向下一个cpool_stat_map的指针

		clear_impl();
	////////如果这块内存被被用过,并且有next,那么需要mount新的page
	/* If there is more to be mounted, do it now */
	} else if (shared_mem_->has_next_) {
		/////把当前这个cpool_stat_map_->next_ = get_new_instance_impl(new_name, false);
		////把当前这个cpool_stat_map_->has_next_ = true
		mount_next(&error);
		ASSERT(next_ != NULL);

	} else { /////这块内存被用过,但没有next
		next_ = NULL;
	}

out:

	isi_error_handle(error, error_out);
}

template <class map_key_class, class value_class>
value_class*
cpool_stat_shared_map<map_key_class, value_class>::at(const map_key_class &k,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	value_class *ret_val = NULL;

	check_next_mounted(&error);
	ON_ISI_ERROR_GOTO(out, error);

	lock_for_write(&error);
	ON_ISI_ERROR_GOTO(out, error);

	cpool_stat_shared_map<map_key_class, value_class> *cur;
	for (cur = this; cur != NULL && ret_val == NULL; cur = cur->next_) {

		ret_val = cur->at_impl(k);

		if (ret_val != NULL)
			break;
	}

	if (ret_val == NULL)
		error = isi_system_error_new(ENOENT, ERROR_NO_ENTRY_FOUND);

	if (error == NULL)
		unlock_for_write(&error);
	else
		unlock_for_write(isi_error_suppress(IL_NOTICE));

out:
	isi_error_handle(error, error_out);

	return ret_val;
}

template <class map_key_class, class value_class>
value_class*
cpool_stat_shared_map<map_key_class, value_class>::at_impl(
    const map_key_class &k)
{
	value_class *ret_val = NULL;
	int index = find_index(k);

	if (index >= 0)
		ret_val = shared_mem_->list_els_[index].get_value();

	return ret_val;
}

template <class map_key_class, class value_class>
void/////当前的cpool_stat_shared_map连接下一个cpool_stat_shared_map
cpool_stat_shared_map<map_key_class, value_class>::mount_next(//////递归往后
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	char new_name[MAX_SHMEM_NAME_LENGTH + 1];

	/*
	 * Create a new unique path by appending a single character to the end
	 * of the previous path
	 */
	sprintf(new_name, "%s%c", shared_mem_->name_, 'x');////每一个共享内存就是cpool_stat_map,有一个自己独特的名字
	////cpool_stat_shared_map<map_key_class, value_class> *next_;  next指针指向下一块cpool_stat_shared_map
	next_ = get_new_instance_impl(new_name, false, &error);
	shared_mem_->has_next_ = true;

	isi_error_handle(error, error_out);
}

template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::check_next_mounted(
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	/* If shared_memory has a next, but it hasn't been mounted here yet */
	if (shared_mem_->has_next_ && next_ == NULL) {
		lock_for_write(&error);
		ON_ISI_ERROR_GOTO(out, error);

		mount_next(&error);

		if (error != NULL)
			unlock_for_write(isi_error_suppress(IL_NOTICE));

		else
			unlock_for_write(&error);

	} else if (next_ != NULL) {
		next_->check_next_mounted(&error);//////递归调用check_next_mounted
		ON_ISI_ERROR_GOTO(out, error);
	}
out:
	isi_error_handle(error, error_out);
}

template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::insert(
    const map_key_class &k, const value_class &v, struct isi_error **error_out)
{
	bool ret_val = false;
	struct isi_error *error = NULL;

	check_next_mounted(&error);
	ON_ISI_ERROR_GOTO(out, error);

	lock_for_write(&error);
	ON_ISI_ERROR_GOTO(out, error);

	cpool_stat_shared_map<map_key_class, value_class> *cur;
	for (cur = this; cur != NULL; cur = cur->next_) {

		/* Cannot insert if this value already exists */
		if (cur->find_impl(k) != NULL) {
			error = isi_system_error_new(EEXIST, ERROR_KEY_EXISTS);
			goto unlock_and_out;
		}
	}

	for (cur = this; cur != NULL && ret_val == false; cur = cur->next_) {

		/*
		 * Next call can only return false if there are no available
		 * slots
		 */
		ret_val = cur->insert_impl(k, v);

		/* If no open slots, make sure next_ is mounted */
		///如果当前stat_map_的50个slot都用完了,且当前的next_为NULL(当前这个stat_map为链表最后一个stat_map),
		///说明需要创建新的page,那么就mount_next()
		if (!ret_val && cur->next_ == NULL) {
			cur->mount_next(&error);//////用一个新的名字(多加一个'x'),创建一个新的page,然后用next_指向这个共享内存
			ON_ISI_ERROR_GOTO(unlock_and_out, error);

			ASSERT(cur->next_ != NULL);
		}
	}

unlock_and_out:
	if (error != NULL)
		unlock_for_write(isi_error_suppress(IL_NOTICE));

	else
		unlock_for_write(&error);

out:
	isi_error_handle(error, error_out);
}

template <class map_key_class, class value_class>
bool
cpool_stat_shared_map<map_key_class, value_class>::insert_impl(
    const map_key_class &k, const value_class &v)
{
	bool ret_val = false;

	for (int i = 0; i < CP_STAT_SHARED_MAP_PAGE_SIZE; i++) {
		if (shared_mem_->list_els_[i].is_empty()) {
			shared_mem_->list_els_[i].set(k, v);
			ret_val = true;
			goto out;
		}
	}

out:
	return ret_val;
}

template <class map_key_class, class value_class>
bool
cpool_stat_shared_map<map_key_class, value_class>::erase(const map_key_class &k,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	bool ret_val = false;

	check_next_mounted(&error);
	ON_ISI_ERROR_GOTO(out, error);

	lock_for_write(&error);
	ON_ISI_ERROR_GOTO(out, error);

	cpool_stat_shared_map<map_key_class, value_class> *cur;
	for (cur = this; cur != NULL && ret_val == false; cur = cur->next_)

		ret_val = cur->erase_impl(k);

	unlock_for_write(&error);

out:
	isi_error_handle(error, error_out);

	return ret_val;
}

template <class map_key_class, class value_class>
bool
cpool_stat_shared_map<map_key_class, value_class>::erase_impl(
    const map_key_class &k)
{
	bool ret_val = false;
	int index = find_index(k);

	if (index >= 0) {
		shared_mem_->list_els_[index].clear();
		ret_val = true;

	}

	return ret_val;
}

template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::clear(
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	check_next_mounted(&error);
	ON_ISI_ERROR_GOTO(out, error);

	lock_for_write(&error);
	ON_ISI_ERROR_GOTO(out, error);

	cpool_stat_shared_map<map_key_class, value_class> *cur;
	for (cur = this; cur != NULL; cur = cur->next_)

		cur->clear_impl();

	unlock_for_write(&error);

out:
	isi_error_handle(error, error_out);
}

template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::clear_impl()
{
	for (int i = 0; i < CP_STAT_SHARED_MAP_PAGE_SIZE; i++)
		shared_mem_->list_els_[i].clear();
}

template <class map_key_class, class value_class>
const value_class *
cpool_stat_shared_map<map_key_class, value_class>::find(const map_key_class &k,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	const value_class *ret_val = NULL;

	check_next_mounted(&error);
	ON_ISI_ERROR_GOTO(out, error);

	lock_for_read(&error);
	ON_ISI_ERROR_GOTO(out, error);

	cpool_stat_shared_map<map_key_class, value_class> *cur;
	for (cur = this; cur != NULL && ret_val == NULL; cur = cur->next_)

		ret_val = cur->find_impl(k);

	unlock_for_read(&error);

out:
	isi_error_handle(error, error_out);

	return ret_val;
}

template <class map_key_class, class value_class>
const value_class *
cpool_stat_shared_map<map_key_class, value_class>::find_impl(
    const map_key_class &k)
{
	const value_class *ret_val = NULL;

	int index = find_index(k);

	if (index >= 0)
		ret_val = shared_mem_->list_els_[index].get_value();

	return ret_val;
}

template <class map_key_class, class value_class>
int
cpool_stat_shared_map<map_key_class, value_class>::find_index(
    const map_key_class &k) const
{
	for (int i = 0; i < CP_STAT_SHARED_MAP_PAGE_SIZE; i++) {
		if (!shared_mem_->list_els_[i].is_empty() &&
			shared_mem_->list_els_[i].get_key()->compare(k) == 0)
			return i;
	}

	return -1;
}

template <class map_key_class, class value_class>
size_t
cpool_stat_shared_map<map_key_class, value_class>::size(
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	size_t ct = 0;

	check_next_mounted(&error);
	ON_ISI_ERROR_GOTO(out, error);

	lock_for_read(&error);
	ON_ISI_ERROR_GOTO(out, error);

	const cpool_stat_shared_map<map_key_class, value_class> *cur;
	for (cur = this; cur != NULL; cur = cur->next_)

		ct += cur->size_impl();

	unlock_for_read(&error);

out:
	isi_error_handle(error, error_out);

	return ct;
}

template <class map_key_class, class value_class>
size_t
cpool_stat_shared_map<map_key_class, value_class>::size_impl() const
{
	size_t ct = 0;

	for (int i = 0; i < CP_STAT_SHARED_MAP_PAGE_SIZE; i++) {
		if (!shared_mem_->list_els_[i].is_empty())
			ct++;
	}

	return ct;
}

template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::reset_after_test(
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	if (next_ != NULL) {
		next_->reset_after_test(&error);
		ON_ISI_ERROR_GOTO(out, error);

		delete next_;
		next_ = NULL;

		shared_mem_->has_next_ = false;
	}

	clear(&error);
	ON_ISI_ERROR_GOTO(out, error);
out:
	isi_error_handle(error, error_out);
}

////////一个cluster中多个node(多个进程)，每个node(进程)中有多个线程.
///1.pthread_mutex_lock(): 先分别从每个进程中的多个线程中选出一个线程拿到锁
///2.lock_for_write(&error):代码走到这步时,n个进程中,每个进程中都只有1个线程。通过信号量(semaphore)去抢这把锁，这样最终这n个进程中只有唯一
///一个进程中的唯一一个线程拿到了锁
template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::lock_map(
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int mtx_ret = 0;
	bool mutex_lock_taken = false;

	/* Take a pthread mutex to lock this instance across sharing threads */
	mtx_ret = pthread_mutex_lock(&mutex_);
	if (mtx_ret != 0) {
		/*
		 * This should only happen when trying to take the lock
		 * multiple times in the same thread
		 */
		error = isi_system_error_new(mtx_ret, ERROR_CANNOT_LOCK);
		goto out;
	}
	mutex_lock_taken = true;

	/* Take a semaphore to lock across all processes */
	lock_for_write(&error);
	ON_ISI_ERROR_GOTO(out, error);

	map_locked_ = true;
out:
	if (error != NULL && mutex_lock_taken) {
		mtx_ret = pthread_mutex_unlock(&mutex_);
		ASSERT(mtx_ret == 0, "error unlocking mutex %d", mtx_ret);
	}

	isi_error_handle(error, error_out);
}

////unlock_map的动作和lock_map相反.先lock_.unlock(),再pthread_mutex_unlock()
template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::unlock_map(
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	int mtx_ret = 0;

	if (!map_locked_) {
		error = isi_system_error_new(ENOENT, ERROR_NOT_LOCKED);
		goto out;
	}

	lock_.unlock(&error);
	ON_ISI_ERROR_GOTO(out, error);

	map_locked_ = false;

	mtx_ret = pthread_mutex_unlock(&mutex_);
	ASSERT(mtx_ret == 0, "error unlocking mutex %d", mtx_ret);
out:
	isi_error_handle(error, error_out);
}

template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::as_vector(
    std::vector<cpool_stat_list_element<map_key_class, value_class>*> &out_vec,
    struct isi_error **error_out)
{
	ASSERT(map_locked_);

	struct isi_error *error = NULL;

	check_next_mounted(&error);
	ON_ISI_ERROR_GOTO(out, error);

	cpool_stat_shared_map<map_key_class, value_class> *cur;
	for (cur = this; cur != NULL; cur = cur->next_)

		cur->as_vector_impl(out_vec);

out:
	isi_error_handle(error, error_out);
}

template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::as_vector_impl(
    std::vector<cpool_stat_list_element<map_key_class, value_class>*> &out_vec)
{
	for (int i = 0; i < CP_STAT_SHARED_MAP_PAGE_SIZE; i++) {
		if (!shared_mem_->list_els_[i].is_empty())
			out_vec.push_back(&shared_mem_->list_els_[i]);
	}
}

template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::keys_as_vector(
    std::vector<const map_key_class*> &out_vec, struct isi_error **error_out)
{
	ASSERT(map_locked_);

	struct isi_error *error = NULL;

	check_next_mounted(&error);
	ON_ISI_ERROR_GOTO(out, error);

	cpool_stat_shared_map<map_key_class, value_class> *cur;
	for (cur = this; cur != NULL; cur = cur->next_)

		cur->keys_as_vector_impl(out_vec);

out:
	isi_error_handle(error, error_out);
}

template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::keys_as_vector_impl(
    std::vector<const map_key_class*> &out_vec)
{
	for (int i = 0; i < CP_STAT_SHARED_MAP_PAGE_SIZE; i++) {
		if (!shared_mem_->list_els_[i].is_empty())
			out_vec.push_back(shared_mem_->list_els_[i].get_key());
	}
}

template <class map_key_class, class value_class>
void
cpool_stat_shared_map<map_key_class, value_class>::preallocate_space_for_tests(
    int num_potential_accounts, struct isi_error **error_out)
{
	ASSERT(num_potential_accounts > 0);
	ASSERT(map_locked_ == false);

	struct isi_error *error = NULL;
	size_t num_entries_populated = 0, num_pages = 0, max_num_entries = 0,
	    available_space = 0, additional_entries_needed = 0,
	    additional_pages_needed = 0;
	cpool_stat_shared_map<map_key_class, value_class> *last_page = NULL;
	bool locked = false;

	lock_map(&error);
	ON_ISI_ERROR_GOTO(out, error);

	locked = true;

	num_entries_populated = size(&error);
	ON_ISI_ERROR_GOTO(out, error);

	/* Count up the number of mounted pages */
	for (cpool_stat_shared_map<map_key_class, value_class> *cur_page = this;
	    cur_page != NULL; cur_page = cur_page->next_) {
		num_pages++;
		last_page = cur_page;
	}

	max_num_entries = CP_STAT_SHARED_MAP_PAGE_SIZE * num_pages;

	available_space = max_num_entries - num_entries_populated;

	/* If there are enough available slots already - bail out */
	if ((int)available_space > num_potential_accounts)
		goto out;

	additional_entries_needed = num_potential_accounts - available_space;

	additional_pages_needed =
	    ceil((double)additional_entries_needed /
		(double)CP_STAT_SHARED_MAP_PAGE_SIZE);

	for (size_t i = 0; i < additional_pages_needed; ++i) {
		/* Should have navigated to last page by now */
		ASSERT(last_page != NULL);
		ASSERT(last_page->next_ == NULL);

		last_page->mount_next(&error);
		ON_ISI_ERROR_GOTO(out, error);

		last_page = last_page->next_;
	}
out:
	if (locked)
		unlock_map(
		    (error == NULL ? &error : isi_error_suppress(IL_NOTICE)));

	isi_error_handle(error, error_out);
}

#endif // __CPOOL_STAT_SHARED_MAP_H__
