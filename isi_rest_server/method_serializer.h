#ifndef __METHOD_SERIALIZER_H__
#define __METHOD_SERIALIZER_H__

#include <semaphore.h>
#include <isi_util_cpp/scoped_lock.h>

#include "request.h"

/******************************************************************************\
 * A REST API method serializer uses one of 3 mechanisms to allow a single
 * request to be executed at a time.
 *
 * mutex: Serialize access between threads in a multi-threaded REST server
 * instance.
 *
 * semaphore: Serialize access between processes in a forked process model REST
 * server instance.
 *
 * <DISABLED>
 * fd: Serialize access across service instances on a cluster by using
 * filesystem advisory locks on /ifs.
 *
 * This is problematic during new cluster setup and when /ifs goes readonly
 * because the class initializer tries to create and/or verify the lock file
 * directory path, which fails if /ifs is not readable. Because this happens
 * during class initialization this prevents API daemons from starting properly.
 * If the assert is removed at initialization and the service starts without
 * creating the lock file directory, subsequent attempts to use open the lock
 * file will fail.
 * The higher level behavior of what to do when lock files cannot be opened
 * needs to be evaluated and fixed up before this mechanism is usable.
 * </DISABLED>
 *
 *
 * The various serialization mechanisms inherit from a base class and overload
 * virtual functions to provide consistent behavior inside the
 * method_serializer abstraction class. The base class is designed to delay
 * initialization of locking mechanisms until the first time the serializer is
 * locked to allow initialization to take place *after* a REST server fork.
 *
 * The scoped_serializer_lock provides scoped locking in a method_serializer,
 * regardless of the underlying serialization mechanism.
\******************************************************************************/

/*
 * Default timeout when attempting to take a serializer lock. This timeout
 * is shorter than the default timeout of Apache to allow the error message
 * to make it back to the client before the connection times out.
 */
#define DEFAULT_METHOD_SERIALIZER_TIMEOUT_S 90

/*
 * Base serializer mechanism class.
 * Specializations should not be used without the method_serializer class, but
 * need to be declared individually to allow sharing between multiple handler
 * classes.
 */
class method_serializer;

class __serializer
{
    friend class method_serializer;

public:
    __serializer(bool initialized = true, bool locked = false) :
        abs_timeout_(), initialized_(initialized), locked_(locked)
    { };

protected:
    /*
     * Virtual methods to be overloaded by specialized serialization mechanism
     * classes. These methods *must* be overloaded.
     */
    virtual void _init_lock(void) = 0;
    virtual void _lock(void) = 0;
    virtual void _unlock(void) = 0;

    struct timespec abs_timeout_;

private:

    bool initialized_;
    bool locked_;

    /* Disable copy and assignment constructors. */
    __serializer(const __serializer &);
    __serializer & operator= (const __serializer &);
};

#if 0
/*
 * Serializer mechanism class specialization.
 * The serializer_fd specialization uses files in .ifsvar to serialize
 * method execution between multiple API services across a cluster.
 */
#define SERIALIZER_FD_LOCK_DIR          "/ifs/.ifsvar/run/api_locks/"
#define SERIALIZER_FD_LOCK_PATH(path)   (SERIALIZER_FD_LOCK_DIR path ".lock")
class serializer_fd : public __serializer
{

public:
    serializer_fd(const char * lock_file_name);
    ~serializer_fd(void);

protected:
    void _init_lock(void);
    void _lock(void);
    void _unlock(void);

private:
    int fd_;
    const char * path_;
};
#endif

/*
 * Serializer mechanism class specialization.
 * The serializer_mutex specialization uses a pthread_mutex_t to serialize
 * method execution between threads.
 */
class serializer_mutex : public __serializer
{

public:
    serializer_mutex(void);
    ~serializer_mutex(void);

protected:
    void _init_lock(void);
    void _lock(void);
    void _unlock(void);

private:
    pthread_mutex_t mtx_;;
};

/*
 * Serializer mechanism class specialization.
 * The serializer_none is non-functional serialization mechanism used as the
 * default mechanism in the uri_handler class.
 */
class serializer_none : public __serializer
{

public:
    serializer_none(void) : __serializer() { };

protected:
    void _init_lock(void);
    void _lock(void);
    void _unlock(void);
};

/*
 * Serializer mechanism class specialization.
 * The serializer_sempahore specialization uses system semaphores to serialize
 * method execution between multiple processes on a single node.
 */
#define SERIALIZER_SEMAPHORE_NAME(name) ("/api_sem." name)
class serializer_semaphore : public __serializer
{
public:
    serializer_semaphore(const char* name);
    ~serializer_semaphore(void);

protected:
    void _init_lock(void);
    void _lock(void);
    void _unlock(void);

private:
    sem_t * sem_;
    const char * name_;
};



/*
 * Methed serialization abstraction class.
 * The method_serializer class syncronizes serialization between HTTP methods
 * using a specialized serialization mechanism class.
 */
class method_serializer
{

public:
    method_serializer(__serializer & serializer);

    void lock(void);
    void unlock(void);

    method_serializer & add_method(request::method method);
    method_serializer & add_all_methods();

    bool serialize(request::method method);

private:
    void _set_all_methods(bool val);

    __serializer & serializer_;
    bool serialize_[request::NUM_METHODS];
};



class scoped_serializer_lock
{
public:
    scoped_serializer_lock(method_serializer &ms, request::method method);
    ~scoped_serializer_lock(void);

private:

    method_serializer serializer_;

    /* disable copy and assignment constructore */
    scoped_serializer_lock(const scoped_serializer_lock &);
    scoped_serializer_lock & operator= (const scoped_serializer_lock &);
};

#endif
