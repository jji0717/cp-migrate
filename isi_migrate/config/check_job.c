#include <unistd.h>

#include <check.h>

#include <isi_util/check_helper.h>
#include <isi_migrate/migr/isirep.h>
#include "siq_source.h"

#define TEST_REPORT_FILE "test_job_report.xml"

static const char example_report[];


TEST_FIXTURE(suite_setup);
TEST_FIXTURE(suite_teardown);

SUITE_DEFINE_FOR_FILE(siq_job, .suite_setup = suite_setup,
    .suite_teardown = suite_teardown);

TEST_FIXTURE(suite_setup)
{
	int len, ret, fd = open(SIQ_IFS_CONFIG_DIR TEST_REPORT_FILE,
	    O_WRONLY | O_CREAT | O_TRUNC, 0666);

	fail_unless(fd > 0);

	len = strlen(example_report);
	ret = write(fd, example_report, len);
	fail_unless(ret == len);

	fsync(fd);
	close(fd);
}

TEST_FIXTURE(suite_teardown)
{
	unlink(SIQ_IFS_CONFIG_DIR TEST_REPORT_FILE);
}

TEST(job_summary_noleak, .attrs="overnight")
{
	//struct siq_job_summary report;
	//int ret;

	//ret = siq_job_summary_loadfrom(SIQ_IFS_CONFIG_DIR, TEST_REPORT_FILE,
	//    &report);
	//fail_unless(ret == 0, "errno = %s", strerror(errno));

	//siq_job_summary_free(&report);
}

static const char example_report[] =
"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
"<job-summary>"
"  <total>"
"    <stat>"
"      <dirs>"
"        <src>"
"          <visited>1</visited>"
"        </src>"
"        <dst>"
"          <deleted>0</deleted>"
"        </dst>"
"      </dirs>"
"      <files>"
"        <total>1010</total>"
"        <replicated>"
"          <selected>0</selected>"
"          <transferred>0</transferred>"
"          <new_files>0</new_files>"
"          <updated_files>0</updated_files>"
"          <files_with_ads>0</files_with_ads>"
"          <ads_streams>0</ads_streams>"
"          <symlinks>0</symlinks>"
"        </replicated>"
"        <deleted>"
"          <src>0</src>"
"          <dst>0</dst>"
"        </deleted>"
"        <skipped>"
"          <up_to_date>1010</up_to_date>"
"          <user_conflict>0</user_conflict>"
"          <error_io>0</error_io>"
"          <error_net>0</error_net>"
"          <error_checksum>0</error_checksum>"
"        </skipped>"
"      </files>"
"      <bytes>"
"        <transferred>12401</transferred>"
"        <recoverable>0</recoverable>"
"        <recovered>"
"          <src>0</src>"
"          <dst>0</dst>"
"        </recovered>"
"        <data>"
"          <total>0</total>"
"          <file>0</file>"
"          <sparse>0</sparse>"
"          <unchanged>0</unchanged>"
"        </data>"
"        <network>"
"          <total>12401</total>"
"          <to_target>12213</to_target>"
"          <to_source>188</to_source>"
"        </network>"
"      </bytes>"
"    </stat>"
"    <start>1267906504</start>"
"    <end>1267906597</end>"
"    <state>success</state>"
"  </total>"
"  <chunks>"
"    <total>0</total>"
"    <running>0</running>"
"    <success>0</success>"
"    <failed>0</failed>"
"  </chunks>"
"  <action>"
"    <type>sync</type>"
"    <assess>false</assess>"
"  </action>"
"  <retry>true</retry>"
"  <error/>"
"  <errors/>"
"  <warnings/>"
"  <snapshots>"
"    <target/>"
"  </snapshots>"
"  <dead_node>false</dead_node>"
"  <num_retransmitted_files>0</num_retransmitted_files>"
"  <retransmitted_files/>"
"  <job_spec>"
"    <spec>"
"      <type>user</type>"
"      <name>new_sched7</name>"
"      <desc/>"
"      <src>"
"        <cluster_name/>"
"        <snapshot_mode>make</snapshot_mode>"
"        <root_path>/ifs/data</root_path>"
"      </src>"
"      <dst>"
"        <cluster_name>10.54.151.151</cluster_name>"
"        <password/>"
"        <snapshot_mode>none</snapshot_mode>"
"        <snapshot_pattern>SIQ-%{SrcCluster}-%{PolicyName}-%Y-%m-%d_%H-%M</snapshot_pattern>"
"        <snapshot_alias>SIQ-%{SrcCluster}-%{PolicyName}-latest</snapshot_alias>"
"        <path>/ifs/sync/new_sched_data7</path>"
"      </dst>"
"      <predicate>-name '*'</predicate>"
"      <check_integrity>true</check_integrity>"
"      <log_level>notice</log_level>"
"     <coordinator>"
"        <workers_per_node>3</workers_per_node>"
"        <recurse_depth>2</recurse_depth>"
"        <ncolors>0</ncolors>"
"      </coordinator>"
"      <make_links>false</make_links>"
"      <link_target/>"
"      <log_removed_files>false</log_removed_files>"
"      <delete_delay>0</delete_delay>"
"      <snapshot_expiration>31536000</snapshot_expiration>"
"      <rotate_report_period>31536000</rotate_report_period>"
"      <max_reports>2000</max_reports>"
"      <siq_v1_lastrun>0</siq_v1_lastrun>"
"      <max_errors>1</max_errors>"
"      <restrict_by/>"
"      <target_restrict>false</target_restrict>"
"      <force_interface>false</force_interface>"
"      <diff_sync>false</diff_sync>"
"      <skip_bb_hash>false</skip_bb_hash>"
"      <disable_stf>false</disable_stf>"
"    </spec>"
"  </job_spec>"
"</job-summary>"
;
