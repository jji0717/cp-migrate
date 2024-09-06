#include <isi_ilog/ilog.h>

#include "api_error.h"
#include "method_stats.h"
#include "uri_manager.h"

uri_manager::uri_manager(const uri_mapper &mapper)
	: mapper_(mapper)
{ }

void
uri_manager::execute_request(request &input, response &output, bool validate_schema) { try
{
	method_timer timer(&mapper_.unknown_stats_);
	input.set_timer(&timer);
	std::string uri_str = request::string_from_method(input.get_method());
	uri_str.append(" ");
	uri_str.append(input.get_uri());
	std::string timing_msg = "starting request to ";
	timing_msg.append(uri_str);
	input.log_timing_checkpoint(timing_msg.c_str());

	/*
	 * Handle directory listing (?describe&list[&all][&internal]) before
	 * actual handler resolution. This allows full directory listing at
	 * the root or other places where no handler resolves.  No
	 * privs are checked.
	 */

	const request::arg_map &args = input.get_arg_map();
	const request::method method = input.get_method();

	unsigned describe_cnt = args.count(uri_handler::describe_option);
	unsigned list_cnt     = args.count(uri_handler::list_option);

	bool describe = method == request::GET && describe_cnt > 0;

	if (describe && list_cnt > 0) {
		bool list_int = args.count(uri_handler::internal_option) > 0;
		mapper_.get_directory(input, output, list_int);
		timer.set_stats(&mapper_.describe_stats_);
	} else if (mapper_.handle_metadata_requests(input, output)) {
		/* A metadata request has been handled by the mapper */
		timer.set_stats(&mapper_.describe_stats_);
	} else {
		/* otherwise do mapping and execution */
		uri_handler *h = mapper_.resolve(input, output);

		if (h == NULL)
			throw api_exception(AEC_NOT_FOUND,
                            get_user_text(REST_PATH_NOT_FOUND_x),
                            input.get_uri().c_str());

		if (describe)
			timer.set_stats(&mapper_.describe_stats_);
		else
			if (method != request::UNSUPPORTED) {
				timer.set_stats(&h->stats_[
				    std::min(method, request::NUM_METHODS)]);
			}
		h->execute_http_method(input, output, validate_schema);
	}

	std::string finished_msg = "finished request to ";
	finished_msg.append(uri_str);
	input.log_timing_checkpoint(finished_msg.c_str());
	timer.set_success();
	input.set_timer(NULL);
}
catch (const api_exception &e)
{
	input.log_request(true);
	output.add_error(e.error_code(), e.what(), e.get_field());
	ilog(IL_DEBUG, "ERROR %s", e.what());
	ILOG_START(IL_REST_ERR_BT) {
		isi_error *error = e.detach();
		fmt_print(ilogfmt, "STACK %#{}", isi_error_fmt(error));
		isi_error_free(error);
	} ILOG_END;
}
catch (const isi_exception &e)
{
	input.log_request(true);
	output.add_error(AEC_EXCEPTION, e.what());
	ilog(IL_INFO, "ERROR %s", e.what());
	ILOG_START(IL_REST_ERR_BT) {
		isi_error *error = e.detach();
		fmt_print(ilogfmt, "STACK %#{}", isi_error_fmt(error));
		isi_error_free(error);
	} ILOG_END;
}
catch (const std::exception &e)
{
	input.log_request(true);
	output.add_error(AEC_EXCEPTION, e.what());
	ilog(IL_INFO, "ERROR %s", e.what());
}
catch (...)
{
	input.log_request(true);
	output.add_error(AEC_EXCEPTION, "Unknown exception");
	ilog(IL_INFO, "ERROR Unknown exception");
}
}
