#include <stdio.h>
#include <dirent.h>

#include "isi_util/isi_format.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#include "bwt_conf.h"
#include "siq_util_local.h"
#include "isi_migrate/sched/siq_log.h"

//Common file locations
#define REPORTDIR "/ifs/.ifsvar/modules/tsm/bwt"
#define BWKEYFILE "/ifs/.ifsvar/modules/tsm/bwt/bandwidth.key"
#define THKEYFILE "/ifs/.ifsvar/modules/tsm/bwt/throttle.key"
#define CPUKEYFILE "/ifs/.ifsvar/modules/tsm/bwt/cpu.key"

//This number can't be too big otherwise WebUI will crash
#define FILE_BUFFER_SIZE (1024 * 32)

//Forward declaration
bwt_report_t* bwt_reports_get(time_t start, time_t end, enum bwt_limit_type type,
    struct isi_error** error_out);
int process_report(bwt_report_t** reportList, bwt_report_t** reportPointer_out,
    struct fmt_vec* keys, char* reportFileName, int startTime, int endTime, 
    int type, struct isi_error** error_out);
int process_report_line(char* reportLine, struct fmt_vec* keys, 
    bwt_report_t* report, struct isi_error** error_out);

struct gci_tree bwt_gc_conf_gci_root = {
	.root = &gci_ivar_bwt_conf,
	.primary_config = "/ifs/.ifsvar/modules/tsm/config/bwt.gc",
	.fallback_config_ro = NULL,
	.change_log_file = NULL
};

struct bwt_conf *
bwt_conf_create(void)
{
	struct bwt_conf *conf = NULL;

	CALLOC_AND_ASSERT(conf, struct bwt_conf);

	return conf;
}

struct bwt_limit *
bwt_limit_create(void)
{
	struct bwt_limit *limit = NULL;
	
	CALLOC_AND_ASSERT(limit, struct bwt_limit);
	CALLOC_AND_ASSERT(limit->schedule, struct bwt_schedule);
	
	return limit;
}

struct bwt_gc_conf *
bwt_gc_conf_load(struct isi_error **error_out)
{
	struct bwt_gc_conf *conf = NULL;
	struct isi_error *error = NULL;
	
	CALLOC_AND_ASSERT(conf, struct bwt_gc_conf);
	
	conf->gci_base = gcfg_open(&bwt_gc_conf_gci_root, 0, &error);
	if (error) {
		log(ERROR, "%s: Failed to gcfg_open: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	conf->gci_ctx = gci_ctx_new(conf->gci_base, false, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	gci_ctx_read_path(conf->gci_ctx, "", &conf->root, &error);
	if (error) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__, 
		    isi_error_get_message(error));
		goto out;
	}
	
out:
	if (error) {
		bwt_gc_conf_free(conf);
		isi_error_handle(error, error_out);
		conf = NULL;
	}
	
	return conf;
}

int 
bwt_gc_conf_save(struct bwt_gc_conf *conf, struct isi_error **error_out) 
{
	int ret = -1;
	struct isi_error *isierror = NULL;
	
	ASSERT(conf->gci_ctx && conf->gci_base);
	
	gci_ctx_write_path(conf->gci_ctx, "", conf->root, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_write_path: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	
	gci_ctx_commit(conf->gci_ctx, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_commit: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	
	gci_ctx_free(conf->gci_ctx);
	conf->gci_ctx = gci_ctx_new(conf->gci_base, false, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_new: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}

	bwt_conf_free(conf->root);
	gci_ctx_read_path(conf->gci_ctx, "", &conf->root, &isierror);
	if (isierror) {
		log(ERROR, "%s: Failed to gci_ctx_read_path: %s", __func__, 
		    isi_error_get_message(isierror));
		goto out;
	}
	
	ret = 0;
out:
	isi_error_handle(isierror, error_out);
	return ret;
}

void
bwt_gc_conf_close(struct bwt_gc_conf *conf)
{
	if (!conf) {
		return;
	}
	
	if (conf->gci_ctx) {
		gci_ctx_free(conf->gci_ctx);
		conf->gci_ctx = NULL;
	}
	if (conf->gci_base) {
		gcfg_close(conf->gci_base);
		conf->gci_base = NULL;
	}
}

void
bwt_gc_conf_free(struct bwt_gc_conf *conf)
{
	if (!conf) {
		return;
	}
	
	bwt_gc_conf_close(conf);

	bwt_conf_free(conf->root);
	FREE_AND_NULL(conf);
}

void
bwt_conf_free(struct bwt_conf *conf)
{
	if (!conf) {
		return;
	}
	
	bwt_limit_free(&conf->bw);
	bwt_limit_free(&conf->th);
	bwt_limit_free(&conf->cpu);
	bwt_limit_free(&conf->work);

	FREE_AND_NULL(conf);
}

void
bwt_limit_free(struct bwt_limit_head *head)
{
	struct bwt_limit *current = NULL;
	
	while (!SLIST_EMPTY(head)) {
		current = SLIST_FIRST(head);
		SLIST_REMOVE_HEAD(head, next);
		FREE_AND_NULL(current->schedule);
		FREE_AND_NULL(current->desc);
		FREE_AND_NULL(current);
	}
}

bwt_report_t * 
bwt_bw_reports_get(time_t start, time_t end, 
    struct isi_error** error_out)
{
	return bwt_reports_get(start, end, BWT_LIM_BW, error_out);
}

bwt_report_t * 
bwt_th_reports_get(time_t start, time_t end, 
    struct isi_error** error_out)
{
	return bwt_reports_get(start, end, BWT_LIM_TH, error_out);
}

bwt_report_t * 
bwt_cpu_reports_get(time_t start, time_t end, 
    struct isi_error** error_out)
{
	return bwt_reports_get(start, end, BWT_LIM_CPU, error_out);
}

bwt_report_t *
bwt_work_reports_get(time_t start, time_t end,
    struct isi_error** error_out)
{
	return bwt_reports_get(start, end, BWT_LIM_WORK, error_out);
}

bwt_report_t * 
bwt_reports_get(time_t start, time_t end, enum bwt_limit_type type, 
    struct isi_error** error_out)
{
	struct fmt_vec keyVector;
	FILE* keyfile;
	int bytesRead;
	char buffer[FILE_BUFFER_SIZE];
	char* bufferToken;
	char* keyContext = NULL;
	int numDirs = 0;
	struct dirent** dirs = NULL;
	bool firstPastEndTime;
	bwt_report_t* toReturn = NULL;
	struct isi_error* error = NULL;
	bwt_report_t* reportPointer = NULL;
	
	//Initialize keyVector
	fmt_vec_init(&keyVector);
	
	//Check args
	if (end < start) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,"Invalid arguments: "
		    "end must be >= start");
		goto done;
	}
	
	//Read+Parse keys
	if (type == BWT_LIM_BW) {
		//Bandwidth
		keyfile = fopen(BWKEYFILE, "r");
	} else if (type == BWT_LIM_TH) {
		//Throttle
		keyfile = fopen(THKEYFILE, "r");
	} else if (type == BWT_LIM_CPU) {
		//CPU
		keyfile = fopen(CPUKEYFILE, "r");
	} else if (type == BWT_LIM_WORK) {
		//Worker
		//No key file for worker pools since we do not collect
		//stats per node
		keyfile = NULL;
	} else {
		ASSERT(type < BWT_LIM_TOTAL);
		error = isi_siq_error_new(E_SIQ_GEN_CONF,"Unknown type: %d", type);
		goto done;
	}

	if (type != BWT_LIM_WORK) {
		if (keyfile == NULL) {
			error = isi_siq_error_new(E_SIQ_GEN_CONF,
			    "Failed to fopen keyfile");
			goto done;
		}
		bytesRead = fread(buffer, 1, FILE_BUFFER_SIZE, keyfile);
		fclose(keyfile);
		keyfile = NULL;

		if (bytesRead <= 0) {
			error = isi_siq_error_new(E_SIQ_GEN_CONF,
			    "Failed to fread from keyfile");
			goto done;
		}

		//Tokenize using "\n"
		bufferToken = strtok_r(buffer, "\n", &keyContext);
		//Iterate and make keys list
		while (bufferToken != NULL) {
			struct fmt key = FMT_INITIALIZER;
			fmt_print(&key, bufferToken);
			fmt_vec_append(&keyVector, &key);
			fmt_clean(&key);
			bufferToken = strtok_r(NULL, "\n", &keyContext);
		}
	}

	if (keyfile == NULL) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF, KEYFILE_OPEN_ERR);
		goto done;
	}

	//Get list of directories in alphabetical order
	numDirs = scandir(REPORTDIR, &dirs, 0, alphasort);
	firstPastEndTime = false;
	for (int dirPointer = 0; dirPointer < numDirs; dirPointer++) {
		char* dirnameContext = NULL;
		char* dirname;
		char* dirnameTok;
		char* typeToSearch;
		int timestamp;
		int prResult;
		
		//Filter out only the directories we want to look at
		//Expected format we are looking for: 
		//[bandwidth|throttle|cpu]-<timestamp>.log
		//Check type first
		dirname = strdup(dirs[dirPointer]->d_name);
		dirnameTok = strtok_r(dirname, "-", &dirnameContext);
		if (dirnameTok == NULL) {
			//Doesn't contain a -
			free(dirname);
			continue;
		}
		
		if (type == BWT_LIM_BW) {
			typeToSearch = "bandwidth";
		} else if (type == BWT_LIM_TH) {
			typeToSearch = "throttle";
		} else if (type == BWT_LIM_CPU) {
			typeToSearch = "cpu";
		} else if (type == BWT_LIM_WORK) {
			typeToSearch = "work";
		} else {
			//Cleanup a ton of stuff
			error = isi_siq_error_new(E_SIQ_GEN_CONF,"Unknown type: %d", type);
			free(dirname);
			goto done;
		}
		if (strcmp(typeToSearch, dirnameTok) != 0) {
			//Does not start with bandwidth throttle cpu or work
			free(dirname);
			continue;
		}
		
		//Timestamp parse
		dirnameTok = strtok_r(NULL, ".", &dirnameContext);
		timestamp = atoi(dirnameTok);
		if (timestamp <= 0) {
			//Does not have a timestamp
			free(dirname);
			continue;
		}
		
		//This file needs to end with .log
		dirnameTok = strtok_r(NULL, ".", &dirnameContext);
		if (strcmp(dirnameTok, "log") != 0) {
			//Does not end in .log
			free(dirname);
			continue;
		}
		
		//Is the file's timestamp > starttime and 
		//(< endtime) or (the first one that is > endtime)?
		if (timestamp >= start && timestamp <= end) {
			//Within time range
			prResult = process_report(&toReturn, &reportPointer, 
			    &keyVector, dirs[dirPointer]->d_name, start, 
			    end, type, &error);
		} else if (timestamp > end && !firstPastEndTime) {
			//Still within time range
			prResult = process_report(&toReturn, &reportPointer, 
			    &keyVector, dirs[dirPointer]->d_name, start, 
			    end, type, &error);
			firstPastEndTime = true;
		} else if (timestamp > end && firstPastEndTime) {
			//Outside range - since this is a sorted list, 
			//we can just stop here
			free(dirname);
			break;
		} else {
			//We are not yet at startTime, 
			//just continue to the next one
			prResult = 0;
		}
		
		if (prResult != 0) {
			//Something went wrong, just bail
			//Error bubbles up from process_report
			free(dirname);
			goto done;
		}
			
		//Next iteration
		free(dirname);
	}

	//If we never hit the endtime, then also parse 
	//the bandwidth.log or throttle.log file
	if (!firstPastEndTime) {
		if (type == BWT_LIM_BW) {
			if (process_report(&toReturn, &reportPointer, 
			    &keyVector, "bandwidth.log", start, end, type, 
			    &error) != 0) {
				goto done;
			}
		} else if (type == BWT_LIM_TH) {
			if (process_report(&toReturn, &reportPointer, 
			    &keyVector, "throttle.log", start, end, type, 
			    &error) != 0) {
				goto done;
			}
		} else if (type == BWT_LIM_CPU) {
			if (process_report(&toReturn, &reportPointer, 
			    &keyVector, "cpu.log", start, end, type, 
			    &error) != 0) {
				goto done;
			}
		} else if (type == BWT_LIM_WORK) {
			if (process_report(&toReturn, &reportPointer,
			    &keyVector, "work.log", start, end, type,
			    &error) != 0) {
				goto done;
			}
		} else {
			//Cleanup a ton of stuff
			goto done;
		}
	}
	
done:
	//Cleanup scandir
	if (dirs) {
		for (int i = 0; i < numDirs; i++) {
			free(dirs[i]);
		}
		free(dirs);
	}
	
	//Cleanup vector
	fmt_vec_clean(&keyVector);

	if (error) {
		if (toReturn != NULL) {
			bwt_reports_free(toReturn);
		}
		isi_error_handle(error, error_out);
		return NULL;
	} else {
		return toReturn;
	}
}

int 
process_report(bwt_report_t** reportList, bwt_report_t** reportPointer_out, 
    struct fmt_vec* keys, char* reportFileName, int startTime, int endTime, 
    int type, struct isi_error** error_out) 
{
	char fullPath[10240];
	char reportBuffer[FILE_BUFFER_SIZE+1] = {};
	int result = -1;
	FILE* reportFile;
	char* currentPosition = NULL;
	struct isi_error* error = NULL;
	int bytesRead = 0;
	int parsedBytes = 0;
	int currentParsedBytes = 0;
	bool retry = true;
	
	sprintf(fullPath, "%s/%s", REPORTDIR, reportFileName);
	
	reportFile = fopen(fullPath, "r");
	if (reportFile == NULL) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,"Failed to fopen report: %s", fullPath);
		goto done;
	}
	memset(reportBuffer, 0, sizeof(reportBuffer));
	bytesRead = fread(reportBuffer, 1, FILE_BUFFER_SIZE, reportFile);
	while (bytesRead > 0) {
		bool parseError = false;
		char currentLine[1024];
		
		while (currentParsedBytes < bytesRead) {
			char timestampStr[1024];
			int timestamp = 0;
			
			memset(currentLine, 0, sizeof(currentLine));
			memset(timestampStr, 0, sizeof(timestampStr));
			
			currentPosition = reportBuffer + currentParsedBytes;
			
			char* newLinePos = strchr(currentPosition, '\n');
			if (newLinePos == NULL) {
				error = isi_siq_error_new(E_SIQ_GEN_CONF,"Failed to "
				    "locate newline");
				parseError = true;
				break;
			}
			
			if (newLinePos - currentPosition >= sizeof(currentLine)) {
				error = isi_siq_error_new(E_SIQ_GEN_CONF,
				    "Line too long");
				parseError = true;
				break;
			}
			strncpy(currentLine, currentPosition, 
			    newLinePos - currentPosition);
			if (strcmp(currentLine, "") == 0) {
				//Blank line, continue
				parsedBytes++;
				currentParsedBytes++;
				parseError = false;
				retry = true;
				continue;
			}
			
			//Iterate over the report lines and check timestamp
			char* spacePos = strchr(currentLine, ' ');
			if(spacePos == NULL) {
				error = isi_siq_error_new(E_SIQ_GEN_CONF,"Failed to locate "
				    "timestamp strchr");
				parseError = true;
				break;
			}
			strncpy(timestampStr, currentLine, 
			    spacePos - currentLine);
			timestamp = atoi(timestampStr);
			//Sanity check
			if (timestamp <= 1000000000) {
				error = isi_siq_error_new(E_SIQ_GEN_CONF,"Failed timestamp "
				    "sanity");
				parseError = true;
				break;
			}
			//If timestamp is within timerange, 
			if (timestamp >= startTime && timestamp <= endTime) {
				//make object + add to list
				bwt_report_t* newReport = calloc(1, 
				    sizeof(bwt_report_t));
				ASSERT(newReport != NULL);
				newReport->type = type;
				if (process_report_line(currentLine, keys, 
				    newReport, &error) != 0) {
					//Bad - error is already populated
					free(newReport);
					parseError = true;
					break;
				}
				if (*reportList == NULL) {
					*reportList = newReport;
					*reportPointer_out = newReport;
				} else {
					(*reportPointer_out)->next = newReport;
					*reportPointer_out = newReport;
				}
			} else if (timestamp > endTime) {
				//No more - stop here
				result = 0;
				goto done;
			} else {
				//timestamp < start
				//No processing needed
			}
			//Next iteration
			parsedBytes += strlen(currentLine) + 1;
			currentParsedBytes += strlen(currentLine) + 1;
			parseError = false;
			retry = true;
		}
		//Handle rewinding/errors
		if (parseError && retry) {
			if (fseek(reportFile, parsedBytes, SEEK_SET) != 0) {
				error = isi_siq_error_new(E_SIQ_GEN_CONF,"Failed to fseek "
				    "report file");
				goto done;
			}
			//Clear isi_error - we'll see this again if it really
			//is a problem
			isi_error_free(error);
			error = NULL;
			retry = false;
		} else if (parseError && !retry) {
			//Error should already be set
			if (!error) {
				error = isi_siq_error_new(E_SIQ_GEN_CONF,"Failed to parse "
				    "report file");
			}
			goto done;
		}
		
		//Read the next chunk
		memset(reportBuffer, 0, sizeof(reportBuffer));
		bytesRead = fread(reportBuffer, 1, FILE_BUFFER_SIZE, 
		    reportFile);
		currentParsedBytes = 0;
	}
	
	result = 0;
done:
	if (reportFile)
		fclose(reportFile);
	isi_error_handle(error, error_out);
	return result;
}

int 
process_report_line(char* reportLine, struct fmt_vec* keys, 
    bwt_report_t* report, struct isi_error** error_out) 
{
	//reportLine is a line straight from the reports on disk
	//Format: <timestamp> <limitAmount> 
	//  (continued)  <allocatedAmount> <totalUsage> [<node1Usage> ...]
	char* context = NULL;
	char* localReportValues;
	char* tokenPointer;
	int keyPointer;
	int ret = -1;
	bwt_report_node_t* nodePointer = NULL;
	struct isi_error* error = NULL;
	
	localReportValues = strdup(reportLine);
	
	//Tokenize string using spaces
	tokenPointer = strtok_r(localReportValues, " ", &context);
	if (tokenPointer == NULL) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,"Failed to parse report file - "
		    "timestamp");
		goto done;
	}
	//Parse timestamp
	report->timestamp = atoi(tokenPointer);
	
	//Parse limitAmount
	tokenPointer = strtok_r(NULL, " ", &context);
	if (tokenPointer == NULL) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,"Failed to parse report file - "
		    "limitAmount");
		goto done;
	}
	report->limit = atol(tokenPointer);
	
	//Parse allocatedAmount
	tokenPointer = strtok_r(NULL, " ", &context);
	if (tokenPointer == NULL) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,"Failed to parse report file - "
		    "allocatedAmount");
		goto done;
	}
	report->allocated = atol(tokenPointer);
	
	//Parse totalUsage
	tokenPointer = strtok_r(NULL, " ", &context);
	if (tokenPointer == NULL) {
		error = isi_siq_error_new(E_SIQ_GEN_CONF,"Failed to parse report file - "
		    "totalUsage");
		goto done;
	}
	report->total = atol(tokenPointer);
	
	//Iterate and parse nodeUsage
	tokenPointer = strtok_r(NULL, " ", &context);
	keyPointer = 0;
	while (tokenPointer != NULL) {
		long cvalue;
		bwt_report_node_t* newReportNode;
		
		if (keyPointer >= fmt_vec_size(keys)) {
			//Parameter mismatch (we have a value, but no 
			//matching node). just stop here
			break;
		}
		
		newReportNode = calloc(1, sizeof(bwt_report_node_t));
		ASSERT(newReportNode != NULL);
		
		//Retrieve key
		newReportNode->node = strdup(
		    fmt_string(&(keys->array[keyPointer])));
		
		//Construct value
		cvalue= atol(tokenPointer);
		newReportNode->usage = cvalue;
		
		//Add to linked list
		if (nodePointer == NULL) {
			nodePointer = newReportNode;
			report->node_usage = nodePointer;
		} else {
			nodePointer->next = newReportNode;
			nodePointer = newReportNode;
		}
		
		//Iterate
		tokenPointer = strtok_r(NULL, " ", &context);
		keyPointer++;
	}
	
	ret = 0;
	
done:
	free(localReportValues);
	isi_error_handle(error, error_out);
	//Return
	return ret;
}

void 
bwt_report_node_free(bwt_report_node_t* node)
{
	bwt_report_node_t* nodePointer = node;
	while (nodePointer) {
		free(nodePointer->node);
		nodePointer->node = NULL;
		bwt_report_node_t* nextRN = nodePointer->next;
		free(nodePointer);
		nodePointer = nextRN;
	}
}

void 
bwt_reports_free(bwt_report_t* report)
{
	bwt_report_t* next = report;
	while (next != NULL) {
		bwt_report_t* saved_next = next->next;
		bwt_report_node_free(next->node_usage);
		next->node_usage = NULL;
		free(next);
		next = saved_next;
	}
}

