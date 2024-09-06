#include <sys/types.h>
#include <sys/stat.h>

#include <string>
#include <boost/filesystem.hpp>
#include <libxml/xmlwriter.h>

#include "report_writer.h"
#include <jsoncpp/json.h> ///jjz

#include <stack>

#define ON_FPRINTF_ERR_GOTO(out_lbl, fpf_ret_val, error, args...) \
	if (fpf_ret_val < 0) { \
		ON_SYSERR_GOTO(out_lbl, errno, error, args); \
	}

/**
 * XML implementation of report_writer
 */
class xml_report_writer : public report_writer {

public:
	xml_report_writer();
	virtual ~xml_report_writer();

	void write_member(const char *member_name,
	    const char *member_value, struct isi_error **error_out);

	void start_member(const char *member_name, struct isi_error **error_out);

	void end_member(struct isi_error **error_out);

	void write_attribute(const char *attribute_name,
	    const char *attribute_value, struct isi_error **error_out);

	void complete_report(struct isi_error **error_out);

protected:

	void open_file(const char *filename, struct isi_error **error_out);

private:
	void indent(struct isi_error **error_out);

    // TODO:: Move filename to base
	const char *filename_;
	FILE *outfile_;
	bool element_open_;
	std::stack<std::string> open_elements_;
};

xml_report_writer::xml_report_writer() : filename_(NULL), outfile_(NULL),
    element_open_(false)
{
}

void
xml_report_writer::indent(struct isi_error **error_out)
{
	ASSERT(outfile_ != NULL);

	struct isi_error *error = NULL;

	for (unsigned int i = 0; i < open_elements_.size(); i++) {
		int n_out = fprintf(outfile_, "\t");
		ON_FPRINTF_ERR_GOTO(out, n_out, error,
		    "Could not write indent to file");
	}
out:
	isi_error_handle(error, error_out);
}

void
xml_report_writer::open_file(const char *filename, struct isi_error **error_out)
{
	//Json::Value eval;
	Json::Value req_json = Json::Value(Json::objectValue);
    req_json["checklist_id"] = "basic";
	ASSERT(filename != NULL);
	ASSERT(outfile_ == NULL, "Cannot open file more than once");

	struct isi_error *error = NULL;
	const char xml_version [] = "1.0";
	const char xml_encoding [] = "UTF-8";
	int n_out = -1;
	char *dirpath = NULL, *path_end = NULL;

	filename_ = strdup(filename);
	ASSERT(filename_ != NULL);

	/* Get the parent directory and make sure it exists first */
	dirpath = strdup(filename);
	ASSERT(dirpath != NULL);

	path_end = strrchr(dirpath, '/');
	if (path_end != NULL) {
		path_end[0] = '\0';
		create_dir(dirpath, &error);
		ON_ISI_ERROR_GOTO(out, error);
	}

	outfile_ = fopen(filename_, "w");
	if (outfile_ == NULL)
		ON_SYSERR_GOTO(out, errno, error, "Could not open file \"%s\"",
		    filename);

	n_out = fprintf(outfile_, "<?xml version=\"%s\" encoding=\"%s\"?>\n",
	    xml_version, xml_encoding);
	ON_FPRINTF_ERR_GOTO(out, n_out, error,
	    "Could not write xml header tag to file");

out:
	free(dirpath);

	isi_error_handle(error, error_out);
}

xml_report_writer::~xml_report_writer()
{
	if (filename_)
		delete filename_;

	if (outfile_ != NULL)
		fclose(outfile_);
// TODO:: Remove failed document from disk?
}

void
xml_report_writer::write_member(const char *member_name,
    const char *member_value, struct isi_error **error_out)
{
	ASSERT(outfile_ != NULL);
	ASSERT(filename_ != NULL);
	ASSERT(member_name != NULL);
	ASSERT(member_value != NULL);

	struct isi_error *error = NULL;
	int n_out = -1;

	if (element_open_) {
		n_out = fprintf(outfile_, " >\n");
		ON_FPRINTF_ERR_GOTO(out, n_out, error,
		    "Could not write member close to file");

		element_open_ = false;
	}
	indent(&error);
	ON_ISI_ERROR_GOTO(out, error);

	n_out = fprintf(outfile_, "<%s>%s</%s>\n", member_name,
	    member_value, member_name);
	ON_FPRINTF_ERR_GOTO(out, n_out, error,
	    "Could not write member to file");
out:
	isi_error_handle(error, error_out);
}

void
xml_report_writer::start_member(const char *member_name,
    struct isi_error **error_out)
{
	ASSERT(outfile_ != NULL);
	ASSERT(filename_ != NULL);
	ASSERT(member_name != NULL);

	struct isi_error *error = NULL;
	int n_out = -1;

	if (element_open_) {
		n_out = fprintf(outfile_, " >\n");
		ON_FPRINTF_ERR_GOTO(out, n_out, error,
		    "Could not write member close to file");

		element_open_ = false;
	}
	indent(&error);
	ON_ISI_ERROR_GOTO(out, error);

	n_out = fprintf(outfile_, "<%s ", member_name);
	ON_FPRINTF_ERR_GOTO(out, n_out, error,
	    "Could not write member open to file");

	element_open_ = true;
	open_elements_.push(member_name);

out:
	isi_error_handle(error, error_out);
}

void
xml_report_writer::end_member(struct isi_error **error_out)
{
	ASSERT(outfile_ != NULL);
	ASSERT(filename_ != NULL);
	ASSERT(open_elements_.size() > 0);

	struct isi_error *error = NULL;
	int n_out = -1;
	std::string element_name = open_elements_.top();

	open_elements_.pop();

	if (element_open_) {
		n_out = fprintf(outfile_, " />\n");
		ON_FPRINTF_ERR_GOTO(out, n_out, error,
		    "Could not write member close to file");

		element_open_ = false;
	} else {
		indent(&error);
		ON_ISI_ERROR_GOTO(out, error);

		n_out = fprintf(outfile_, "</%s>\n", element_name.c_str());
		ON_FPRINTF_ERR_GOTO(out, n_out, error,
		    "Could not write member to file");
	}

out:
	isi_error_handle(error, error_out);
}

void
xml_report_writer::write_attribute(const char *attribute_name,
	    const char *attribute_value, struct isi_error **error_out)
{
	ASSERT(outfile_ != NULL);
	ASSERT(filename_ != NULL);
	ASSERT(element_open_);

	struct isi_error *error = NULL;

	int n_out = fprintf(outfile_, " %s=\"%s\" ", attribute_name,
	    attribute_value);
	ON_FPRINTF_ERR_GOTO(out, n_out, error,
	    "Could not write attribute to file");

out:
	isi_error_handle(error, error_out);
}

void
xml_report_writer::complete_report(struct isi_error **error_out)
{
	ASSERT(outfile_ != NULL);
	ASSERT(filename_ != NULL);

	struct isi_error *error = NULL;

	unsigned int last_open_elements_size = open_elements_.size();
	while (last_open_elements_size > 0) {

		end_member(&error);
		ON_ISI_ERROR_GOTO(out, error);

		ASSERT(open_elements_.size() == last_open_elements_size - 1);
		last_open_elements_size = open_elements_.size();
	}

	fclose(outfile_);
	outfile_ = NULL;
out:
	isi_error_handle(error, error_out);
}

report_writer*
report_writer::create_report_writer(const char *filename,
    struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	report_writer *writer = NULL;

	writer = new xml_report_writer();
	ASSERT(writer != NULL);

	writer->open_file(filename, &error);
	ON_ISI_ERROR_GOTO(out, error);

out:
	if (error != NULL) {
		delete writer;
		writer = NULL;
	}

	isi_error_handle(error, error_out);

	return writer;
}


void
report_writer::encode_string(char *str_in, char *str_out)
{
	ASSERT(str_in != NULL);
	ASSERT(str_out != NULL);

	char *str_token = NULL, *last;
	char *dest = str_out;
	const char enc_newline [] = "\\n";
	bool first_time = true;

	for (str_token = strtok_r(str_in, "\n", &last); str_token != NULL;
	    str_token = strtok_r(NULL, "\n", &last)) {

		if (!first_time) {
			strcpy(dest, enc_newline);
			dest += strlen(enc_newline);
		}

		strcpy(dest, str_token);
		dest += strlen(str_token);

		first_time = false;
	}

	dest[0] = 0;

	ASSERT(strlen(str_out) >= strlen(str_in));
}

void
report_writer::create_dir(const char *path_in, struct isi_error **error_out)
{
	ASSERT(path_in != NULL);

	if (strlen(path_in) == 0)
		return;

	struct isi_error *error = NULL;
	try {
		boost::filesystem::create_directories(path_in);
	} catch (boost::filesystem::filesystem_error &e) {
		ON_SYSERR_GOTO(out, e.code().value(), error,
		    "Boost failed to create path %s: %s", path_in, e.what());
	} catch (std::exception &e) {
		ASSERT(false, "Unexpected exception: %s", e.what());
	}

 out:

	isi_error_handle(error, error_out);
}
