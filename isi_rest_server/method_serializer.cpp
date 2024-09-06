
#include <boost/filesystem.hpp>

#include <assert.h>
#include <sys/file.h>
#include <sys/stat.h>

#include <isi_ufp/isi_ufp.h>

#include "api_error.h"
#include "method_serializer.h"



#if 0
serializer_fd::serializer_fd(const char * path) :
    __serializer(false, false), fd_(-1), path_(path)
{
    boost::filesystem::path lock_dir(SERIALIZER_FD_LOCK_DIR);

    boost::system::error_code ec;
    boost::filesystem::create_directories(lock_dir, ec);

    ASSERT(ec.value() == 0,
            "failed to guarantee lock file directory: %s - %d",
            SERIALIZER_FD_LOCK_DIR,
            ec.value());
};

serializer_fd::~serializer_fd(void)
{
    if (fd_ != -1)
    {
        close(fd_);
        fd_ = -1;
    }
}

void
serializer_fd::_init_lock(void)
{
    if (fd_ == -1)
    {
        fd_ = open(path_, O_WRONLY | O_CREAT, S_IRUSR);
        ASSERT(fd_ != -1,
                "failed to open lock file: %s - %d",
                path_,
                fd_);
    }
};

void
serializer_fd::_lock(void)
{
    int ret = flock(fd_, LOCK_EX);
    ilog_tags(IL_DEBUG, "method_serializer", "Locking file: %s", path_);
    ASSERT(ret == 0,
            "failed to take lock on lock file: %s - %d",
            path_,
            ret);
};

void
serializer_fd::_unlock(void)
{
    int ret = flock(fd_, LOCK_UN);
    ilog_tags(IL_DEBUG, "method_serializer", "Unlocking file: %s", path_);
    ASSERT(ret == 0,
            "failed to remove lock on lock file: %s - %d",
            path_,
            ret);
};
#endif



serializer_mutex::serializer_mutex(void) :
    __serializer(true, false), mtx_()
{
    int ret = pthread_mutex_init(&mtx_, NULL);
    ASSERT(ret == 0, "failed to initialize mutex: %d", ret);
};

serializer_mutex::~serializer_mutex(void)
{
    pthread_mutex_destroy(&mtx_);
}

void
serializer_mutex::_init_lock(void)
{ };

void
serializer_mutex::_lock(void)
{
    int ret = pthread_mutex_timedlock(&mtx_, &abs_timeout_);
    ilog_tags(IL_DEBUG, "method_serializer", "Locking mutex: %p", &mtx_);

    if (ret != 0)
    {
        ilog_tags(IL_ERR, "method_serializer",
                "Failed to lock mutex: [%d] %p: [%i] %s",
                ret, &mtx_, errno, strerror(errno));

        throw api_exception(AEC_SYSTEM_INTERNAL_ERROR,
                "Unable to safely syncronize request. Please try again.");
    }
};

void
serializer_mutex::_unlock(void)
{
    int ret = pthread_mutex_unlock(&mtx_);
    ilog_tags(IL_DEBUG, "method_serializer", "Unlocking mutex: %p", &mtx_);
    ASSERT(ret == 0,
            "failed to unlock mutex: %p - %d",
            &mtx_,
            ret);
};



void
serializer_none::_init_lock(void)
{ };

void
serializer_none::_lock(void)
{ };

void
serializer_none::_unlock(void)
{ };



serializer_semaphore::serializer_semaphore(const char * name) :
    __serializer(false, false), sem_(nullptr), name_(name)
{ };

serializer_semaphore::~serializer_semaphore(void)
{
    sem_unlink(name_);
}

void
serializer_semaphore::_init_lock(void)
{
    sem_ = sem_open(name_, O_CREAT, S_IWUSR, 1);
    ASSERT(sem_ != SEM_FAILED, "failed to open semaphore: %s", name_);
}

void
serializer_semaphore::_lock(void)
{
    int ret = 0;
    while ((ret = sem_timedwait(sem_, &abs_timeout_)) == -1 && errno == EINTR);
    ilog_tags(IL_DEBUG, "method_serializer", "Locking semaphore: %s", name_);

    if (ret != 0)
    {
        ilog_tags(IL_ERR, "method_serializer",
                "Failed to lock semaphore: [%d] %s: [%i] %s",
                ret, name_, errno, strerror(errno));

        throw api_exception(AEC_SYSTEM_INTERNAL_ERROR,
                "Unable to safely syncronize request. Please try again.");
    }
}

void
serializer_semaphore::_unlock(void)
{
    int ret = sem_post(sem_);
    ilog_tags(IL_DEBUG, "method_serializer", "Unlocking semaphore: %s", name_);
    ASSERT(ret == 0,
            "failed to unlock semaphore: %s - %d",
            name_,
            ret);
}



inline static int
method_serializer_fail(void)
{
    UFAIL_POINT_RETURN(rest_method_serializer_fail);
    return 0;
}

inline static int
method_serializer_timeout(void)
{
    UFAIL_POINT_RETURN(rest_method_serializer_timeout);
    return DEFAULT_METHOD_SERIALIZER_TIMEOUT_S;
}

method_serializer::method_serializer(__serializer & serializer) :
    serializer_(serializer)
{ };

void
method_serializer::lock(void)
{
    if (!serializer_.initialized_)
    {
        serializer_._init_lock();
    }

    if (method_serializer_fail())
    {
        throw api_exception(AEC_SYSTEM_INTERNAL_ERROR,
                "[failpoint] intentionally failing to take serializer lock");
    }
    else
    {
        ASSERT(clock_gettime(CLOCK_REALTIME, &serializer_.abs_timeout_) != -1,
                "Failed to read CLOCK_REALTIME");

        serializer_.abs_timeout_.tv_sec += method_serializer_timeout();
        serializer_._lock();
        serializer_.locked_ = true;
    }
}

void
method_serializer::unlock(void)
{
    if (serializer_.locked_)
    {
        serializer_.locked_ = false;
        serializer_._unlock();
    }
}

method_serializer &
method_serializer::add_method(request::method method)
{
    assert(method >= request::GET && method < request::NUM_METHODS);
    serialize_[method] = true;
    return *this;
}

method_serializer &
method_serializer::add_all_methods()
{
    _set_all_methods(true);
    return *this;
}

bool
method_serializer::serialize(request::method method)
{
    assert(method >= request::GET && method < request::NUM_METHODS);
    return serialize_[method];
}

void
method_serializer::_set_all_methods(bool val)
{
    serialize_[request::GET] = val;
    serialize_[request::PUT] = val;
    serialize_[request::POST] = val;
    serialize_[request::DELETE] = val;
    serialize_[request::HEAD] = val;
}



scoped_serializer_lock::scoped_serializer_lock(
        method_serializer &ms, request::method method) :
    serializer_(ms)
{
    if (serializer_.serialize(method))
    {
        serializer_.lock();
    }
}

scoped_serializer_lock::~scoped_serializer_lock(void)
{
    serializer_.unlock();
}

