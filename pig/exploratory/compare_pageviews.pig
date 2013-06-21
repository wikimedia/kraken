set pig.exec.nocombiner true

REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'
REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'

DEFINE COMPARE        org.wikimedia.analytics.kraken.pig.ComparePageviewDefinitions();
DEFINE DATE_BUCKET    org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd');

log_fields    = LOAD '/wmf/raw/webrequest/webrequest-wikipedia-mobile/dt=2013-05-3*' USING PigStorage('\t') AS (
    kafka_offset:double,
    hostname:chararray,
    sequence:long,
    timestamp:chararray,
    request_time:chararray,
    remote_addr:chararray,
    http_status:chararray,
    bytes_sent:long,
    request_method:chararray,
    uri:chararray,
    proxy_host:chararray,
    content_type:chararray,
    referer:chararray,
    x_forwarded_for:chararray,
    user_agent:chararray,
    accept_language:chararray,
    x_cs:chararray
);

log_fields = FOREACH log_fields GENERATE
    DATE_BUCKET(timestamp) AS date_bucket:chararray,
    *;

log_fields = GROUP log_fields BY date_bucket;

counts = FOREACH log_fields GENERATE
    date_bucket,
    FLATTEN(COMPARE(uri,
                    referer,
                    user_agent,
                    http_status,
                    remote_addr,
                    content_type,
                    request_method))
        AS (strict:int, webstatscollector:int, wikistats:int);


grouped_counts = FOREACH counts GENERATE
    date_bucket,
    SUM(strict) as sum_strict:int,
    SUM(webstatscollector) as sum_webstatscollector:int,
    SUM(wikistats) as sum_wikistats:int;


DUMP grouped_counts;

