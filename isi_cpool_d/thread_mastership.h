#ifndef __THREAD_MASTERSHIP_H__
#define __THREAD_MASTERSHIP_H__

/**
 * The default sleep interval in seconds between attempts to acquire mastership
 *
 * @author ssasson (4/30/2014)
 */
const int ACQUIRE_MASTERSHIP_SLEEP_INTERVAL = 60;
/**
 * This function tries to acquire mastership. If it fails it
 * returns false. If other node is chosen to be the master it
 * returns false. Otherwise if node is master it will return true.
 *
 * @param lock_fd_in --- cached fd of the lockfile.
 * @param lockfile --- path for opening the lockfile (if it isn't already open)
 * @param masterfile --- A path to the file where node id of the
 *                       master is written
 *
 */
///lockfile:lockfile的path(/ifs/.ifsvar/modules/cloud/***.lock)
///masterfile: 拿到mastership的这个node把自己的devid写进masterfile(/ifs/.ifsvar/modules/cloud/***.txt)
bool acquire_mastership(int *lock_fd_in, const char *lockfile,
    const char *masterfile);
#endif
