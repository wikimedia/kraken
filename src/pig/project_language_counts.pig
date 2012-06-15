-- Usage:
-- pig -p INPUT=/hdfs/path/to/access_logs* -p OUTPUT=/hdfs/path/to/output_directory -f project_language_counts.pig

DEFINE EXTRACT org.apache.pig.builtin.REGEX_EXTRACT_ALL();

log_fields = LOAD '$INPUT' USING PigStorage(' ') AS (hostname:chararray, udplog_sequence:chararray, timestamp:chararray, request_time:chararray, remote_addr:chararray, http_status:chararray, bytes_sent:chararray, request_method:chararray, uri:chararray, proxy_host:chararray, content_type:chararray, referer:chararray, x_forwarded_for:chararray, user_agent);
PROJECT_LANGUAGE = FOREACH log_fields GENERATE FLATTEN (EXTRACT(uri, 'https?://(..)\\.wikipedia.+')) as (project_language:chararray);
LANGUAGE_COUNT = FOREACH (GROUP PROJECT_LANGUAGE BY $0 PARALLEL 72) GENERATE $0, COUNT($1) as num;
LANGUAGE_COUNT_SORTED = ORDER LANGUAGE_COUNT BY num DESC;
STORE LANGUAGE_COUNT_SORTED into '$OUTPUT';
