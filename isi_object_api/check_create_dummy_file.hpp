#ifndef __CHECK_CREATE_DUMMY_FILE__
#define __CHECK_CREATE_DUMMY_FILE__

#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

#define DUMMY_TEST_FILE "/ifs/test_dummy_readme_file.txt"
static inline void create_dummy_test_file()
{
	int status = 0;
	status = remove(DUMMY_TEST_FILE);
	fail_if((status == -1 && errno != ENOENT),
	    "Removal of file %s failed with error: %s",
	    DUMMY_TEST_FILE, strerror(errno));

	int fd = open(DUMMY_TEST_FILE, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	fail_if((fd == -1),
	    "Creation of file %s failed with error: %s",
	    DUMMY_TEST_FILE, strerror(errno));

	const char *buffer = "This is a test file";
	size_t len = strlen(buffer);

	status = write(fd, buffer, len);
	fail_if((status == -1),
	    "writing  to file %s failed with error: %s",
	    DUMMY_TEST_FILE, strerror(errno));

	close(fd);
}

static inline void remove_dummy_test_file()
{
	int status = 0;
	status = remove(DUMMY_TEST_FILE);
	fail_if((status == -1 && errno != ENOENT),
	    "Removing of file %s failed with error: %s",
	    DUMMY_TEST_FILE, strerror(errno));
}
#endif
