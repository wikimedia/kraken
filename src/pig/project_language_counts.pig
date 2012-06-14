-- Usage:
-- pig -p INPUT=/hdfs/path/to/access_logs* -p OUTPUT=/hdfs/path/to/output_directory

DEFINE EXTRACT org.apache.pig.builtin.REGEX_EXTRACT_ALL();
RAW_LOGS  = LOAD '$INPUT' USING TextLoader as (line:chararray);
-- extract the project language from the url as the 10th match
LOGS_BASE = foreach RAW_LOGS generate FLATTEN (EXTRACT (line, '^(.+) (.+) (.+) (.+) (.+) (.+) (.+) (.+) (https?://(..)\\.wikipedia.+) (.+) (.+) (.+) (.+) (.+)')) as (hostname:chararray, udplog_sequence:chararray, timestamp:chararray, request_time:chararray, remote_addr:chararray, http_status:chararray, bytes_sent:chararray, request_method:chararray, uri:chararray, project_language:chararray, proxy_host:chararray, content_type:chararray, referer:chararray, x_forwarded_for:chararray, user_agent:chararray);
PROJECT_LANGUAGE = FOREACH LOGS_BASE GENERATE project_language;
LANGUAGE_COUNT = FOREACH (GROUP PROJECT_LANGUAGE BY $0 PARALLEL 20) GENERATE $0, COUNT($1) as num;
LANGUAGE_COUNT_SORTED = ORDER LANGUAGE_COUNT BY num DESC;
STORE LANGUAGE_COUNT_SORTED into '$OUTPUT';