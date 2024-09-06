#ifndef CPOOL_D_REPORT_WRITER
#define CPOOL_D_REPORT_WRITER

#include <isi_util/isi_assert.h>
#include <isi_util/isi_error.h>
#include <isi_util/util.h>

class report_writer {

public:
	virtual ~report_writer() {};

	static report_writer *create_report_writer(const char *filename,
	    struct isi_error **error_out);

// TODO:: write array?
	virtual void write_member(const char *member_name,
	    const char *member_value, struct isi_error **error_out) = 0;

	inline void write_member(const char *member_name,
	    uint64_t member_value, struct isi_error **error_out)
	{
		static char buffer[64];
		sprintf(buffer, "%llu", member_value);
		write_member(member_name, buffer, error_out);
	};

	virtual void start_member(const char *member_name,
	    struct isi_error **error_out) = 0;

	virtual void end_member(struct isi_error **error_out) = 0;

	virtual void write_attribute(const char *attribute_name,
	    const char *attribute_value, struct isi_error **error_out) = 0;

	inline void write_attribute(const char *attribute_name,
	    uint64_t attribute_value, struct isi_error **error_out)
	{
		static char buffer[64];
		sprintf(buffer, "%llu", attribute_value);
		write_attribute(attribute_name, buffer, error_out);
	};

	virtual void complete_report(struct isi_error **error_out) = 0;

	/*
	 * Utility function for replacing special characters in str_in
	 * (Note* not used by report_writer, consumer must call if desired)
	 */
	static void encode_string(char *str_in, char *str_out);

protected:
	report_writer() {};

	/*
	 * Creates the given directory if it doesn't exist (including parent
	 * directories)
	 */
	void create_dir(const char *path, struct isi_error **error_out);

	virtual void open_file(const char *filename,
	    struct isi_error **error_out) = 0;
};

#endif // CPOOL_D_REPORT_WRITER
