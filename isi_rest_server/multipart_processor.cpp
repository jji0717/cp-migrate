#include <sys/time.h>

#include <errno.h>
#include <string.h>

#include <vector>

#include <isi_ilog/ilog.h>

#include "multipart_processor.h"

#define MULTI_PART_FORM_DATA	"multipart/form-data"
#define PARAM_BOUNDARY	"boundary"
#define MAX_BOUNDARY 70
#define CRLF 	"\r\n"
#define DASH 	"--"

#define MULTI_PART_FORM_DATA_LEN (sizeof(MULTI_PART_FORM_DATA)-1)
#define SIZE_CRLF	(sizeof(CRLF) -1)
#define HEADER_LIMIT	8190
#define BUFF_SIZE	8192

// Anonymous namespace
namespace {

/**
 * Format the HTTP header similar to what Apache does:
 * All alphabets converted to UPPER case, all other characters
 * converted to underscore "_"
 */
void
format_header(std::string &header)
{
	if (header == api_header::CONTENT_TYPE)
		return;

	char *data = (char *)header.c_str();
	for (size_t i = 0; i < header.size(); ++i) {
		char &ch = *(data + i);
		if (isalpha(ch)) {
			if (!isupper(ch))
				ch = toupper(ch);
		}
		else if (!isdigit(ch))
			ch = '_';		
	}	
}

/**
 * Check if the given content-type is multipart/form-data
 * If so, return  true and set the boundary.
 * See http://www.ietf.org/rfc/rfc2388.txt
 * http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.7
 */
bool
is_multipart_form_data(const std::string &content_type, std::string &boundary)
{
	std::vector<std::string> fields = request::tokenize(content_type, 
	    ";", false);

	for (size_t i = 0; i < fields.size(); ++i) {
		const std::string &str = fields[i];

		if (i == 0 && str != MULTI_PART_FORM_DATA)
			return false;
		
		std::vector<std::string> pair =
		    request::tokenize(str, "=", false);

		if (pair.size() != 2)
			continue;

		std::string field;

		request::trim(pair[0], field);

		if (field == PARAM_BOUNDARY) {
			std::string val;
			request::trim(pair[1], val);

			// remove the quotes:
			if (val.size() > 1 && val[0] == '\"' &&
			    val[val.size() - 1] == '\"')
				boundary = val.substr(1, val.size() - 2);
			else
				boundary = val;
			
			return true;
		}

	}

	return false;
}

} /*end of anonymous namespace*/

void
ipart_handler::set_header(const std::string &header, const std::string &value)
{
	header_map_[header] = value;
}

bool
ipart_handler::get_header(const std::string &header, std::string &val) const
{
	bool found = false;
	std::map<std::string, std::string>::const_iterator it;

	it = header_map_.find(header);
	if (it != header_map_.end()) {
		val = it->second;
		found = true;
	}

	return found;
}

void
ipart_handler::clear()
{
	header_map_.clear();
}

multipart_procecssor::multipart_procecssor(
    const request &input, 
    response &output,
    imultipart_handler &handler) 
    : input_(input), output_(output), handler_(handler)
{
	
}

/*
 * The core routine to process the multipart data.
 */
void
multipart_procecssor::process()
{
	const request &input = input_;
	response &output = output_;
	
	std::vector<char> buff_vec(BUFF_SIZE, 0);

	char *buff = &buff_vec.at(0);
	ASSERT(buff);

	std::string content_type;
	std::string bound_str;
	bool support_multi_form = true;
	bool form_data = false;
	std::string boundary = DASH;
	std::string base_bound = CRLF;
	base_bound += DASH;

	if (support_multi_form &&
	    input.get_header(api_header::CONTENT_TYPE, content_type) &&
	    is_multipart_form_data(content_type, bound_str)) {
		
		if (bound_str.size() > MAX_BOUNDARY) {
			throw api_exception(AEC_BAD_REQUEST,
			    "Boundary string is too long.");
		}
		
		form_data = true;
		boundary += bound_str;
		boundary += CRLF;
		base_bound += bound_str;
	}

	off_t len = input.content_length();
	size_t read_len = 0;
	timeval last_read_time = {0};

	// modelling data processing state:
	enum DATA_STATE {
		DS_PREAMBLE = 0,
		DS_META = 1,
		DS_DATA = 2
	};

	DATA_STATE ds = form_data ? DS_PREAMBLE : DS_DATA;
	bool to_read = true;
	bool form_end = false;

	// indicate data unprocessed in the last read.
	// for boundary checking purpose.
	char *buff_start = buff;
	char *meta_data = NULL;
	char *buff_end = NULL;
	char *write_start = NULL;

	ipart_handler *phandler = NULL;
	if (!form_data) {
		phandler = handler_.on_new_part();
		if (!phandler)
			return;
		if (!phandler->on_headers_ready())
			return;
	}
	
	while (len > 0 || (form_data && !to_read && !form_end)) {
		
		if (to_read) {
			size_t avail_size = BUFF_SIZE - (buff_start - buff);
			read_len = ((off_t)avail_size < len ? avail_size : len);
			size_t len_got = input.read_body(buff_start, read_len);

			timeval read_time = {0};
			gettimeofday(&read_time, NULL);
			if (len_got != read_len) {
				timeval elapse = {0};
				timersub(&read_time, &last_read_time, &elapse);
				output.set_status(ASC_BAD_REQUEST);
				std::string error_info;
				input.get_error(error_info);

				ilog(IL_DEBUG, "Error reading message data "
				    "expecting:%zu, got:%zu, content:%llu, "
				    "read_so_far:%llu errno:%d, "
				    "elapse since last read:%zu(sec), "
				    "last read time:%zu, "
				    "info:%s req:%p uri:%s",
				    read_len, len_got,
				    input.content_length(),
				    input.content_length() - len + len_got,
				    errno,
				    elapse.tv_sec,
				    last_read_time.tv_sec,
				    error_info.c_str(),
				    &input, input.get_uri().c_str());

				throw api_exception(AEC_BAD_REQUEST,
				    "Error reading message data");
			}
			buff_end = buff_start + read_len;
			len -= read_len;
			last_read_time = read_time;
		}

		// need to process multi-part data.
		switch (ds) {

		case DS_PREAMBLE: {
			char *b_start = strnstr(buff, boundary.c_str(),
			    buff_end - buff);
			if (b_start == NULL) {
				size_t left_over_len =
				   boundary.size() - 1;
				// a boundary not found in the preamble
				memcpy (buff, buff_start + read_len -
				    left_over_len,
				    left_over_len
				    );
				buff_start = buff + left_over_len;
				continue;
			}
			meta_data = buff_start + boundary.size();
			ds = DS_META;
			// fall through
			phandler = handler_.on_new_part();
			if (!phandler)
				return;
		}
		case DS_META: {

			char *sep = strnstr(meta_data, CRLF,
			    buff_end - meta_data);

			size_t meta_len = buff_end - meta_data;

			if (!sep) {
				if (meta_len > HEADER_LIMIT) {
					// the header line is too big.
					throw api_exception(AEC_BAD_REQUEST,
					    "Header is too long");
				}
				// not found, continue
				strncpy (buff, meta_data,
				    buff_end - meta_data
				    );
				buff_start = buff + meta_len;
				to_read = true;
				meta_data = buff;
				continue;
			}

			meta_len = sep - meta_data;

			if (meta_len == 0) {
				// this means the end of meta data
				write_start = meta_data + sizeof(CRLF) - 1;
				ds = DS_DATA;
				
				phandler->on_headers_ready();
			}
			else {

				char *cln_pos = strnstr(meta_data, ":",
				    meta_len);

				if (cln_pos == NULL || cln_pos <= meta_data) {
					// the header line is too big.
					throw api_exception(AEC_BAD_REQUEST,
					    "Invalid Header, must have :");
				}

				// handle this header
				std::string header;
				std::string val;

				header.assign(meta_data, cln_pos - meta_data);

				format_header(header);
				
				ssize_t val_len = sep - (cln_pos + 1);
				if (val_len > 0) {
					val.assign(cln_pos+1, val_len);
				}
				
				phandler->set_header(header, val);
				
				// continue process meta data
				meta_data = sep + sizeof (CRLF) - 1;
				if (meta_data >= buff_end) {
					buff_start = buff;
					meta_data = buff;
					to_read = true;
				}
				else {
					// there are unprocessed meta-data
					// continue
					to_read = false;
				}
				continue;
			}
			// fall through;
		}
		case DS_DATA: {

			if (!form_data)
				write_start = buff_start;

			size_t write_len = buff_end - write_start;
			size_t left_over_len = 0;
			char *new_b = NULL;

			if (form_data) {

				// check if there is a boundary in the data
				new_b = (char *)memmem(write_start, write_len,
				    base_bound.c_str(), base_bound.size());

				if (new_b) {
					write_len = new_b - write_start;
					if (buff_end - new_b -
					    base_bound.size() < 2)
						// loaded data cannot
						// indicate if it is new
						// boundary or end
						left_over_len =
						    buff_end - new_b;
				}
				else if (write_len < base_bound.size()) {
					left_over_len = write_len;
					write_len = 0;
				}
				else {
					left_over_len = base_bound.size() - 1;
					write_len = write_len - left_over_len;
				}
			}

			if (write_len > 0) {
				phandler->on_new_data(write_start, write_len);
			}

			if (!form_data)
				continue;

			if (left_over_len > 0) {
				to_read = true; // to read more data.
				// copy the data not written
				memcpy (buff, buff_end - left_over_len,
				    left_over_len);

				buff_start = buff + left_over_len;
				meta_data = NULL;
				write_start = buff;
				continue;
			}


			ASSERT(new_b);
			ASSERT(buff_end - new_b - base_bound.size() >= 2);
			if (!strncmp(new_b + base_bound.size(),
			    CRLF, sizeof(CRLF) - 1 )) {
				// new boundary
				
				meta_data = new_b + base_bound.size() + 2;
				to_read = false;
				ds = DS_META;
				
				phandler->on_data_end();
				handler_.on_part_end(phandler);
				
				phandler = handler_.on_new_part();
				
				if (!phandler)
					return;
					
				continue;
			}

			if (!strncmp(new_b + base_bound.size(),
			    DASH, sizeof(DASH) - 1 )) {
				// end of multi-form data
				form_end = true;				
				phandler->on_data_end();
				handler_.on_part_end(phandler);
				continue;
			}

			throw api_exception(AEC_BAD_REQUEST,
			    "Wrong boundary");
			/* exit */
		}
		default:
			ASSERT("Unexpected case");
			break;
		}
	}
	
	if (!form_data) {
		phandler->on_data_end();
		handler_.on_part_end(phandler);
	}
	
	handler_.on_end();
}
