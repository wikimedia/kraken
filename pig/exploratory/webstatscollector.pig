set pig.exec.nocombiner true

REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'
REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'

DEFINE WEBSTATSCOLLECTOR  org.wikimedia.analytics.kraken.pig.PageViewFilterFunc('webstatscollector');
DEFINE CANONICALIZE       org.wikimedia.analytics.kraken.pig.CanonicalizeArticleTitle('webstatscollector');
DEFINE DATE_BUCKET        org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd_HH');

log_fields    = LOAD '/wmf/raw/webrequest/webrequest-wikipedia-mobile/dt=2013-05-31*' USING PigStorage('\t') AS (
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
    FLATTEN(CANONICALIZE(uri)) AS (title:chararray,language:chararray,project:chararray),
    FLATTEN(WEBSTATSCOLLECTOR(uri,
            referer,
            user_agent,
            http_status,
            remote_addr,
            content_type,
            request_method)) AS (webstatscollector:boolean);

log_fields = FILTER log_fields BY (webstatscollector IS NOT NULL) AND (webstatscollector==TRUE) AND (language=='en') AND (project=='wikipedia.org');

log_fields = GROUP log_fields BY (date_bucket, language, project);

grouped_counts = FOREACH log_fields GENERATE
    FLATTEN(group),
    COUNT($1) AS pageviews:long;

grouped_counts = ORDER grouped_counts BY pageviews DESC;

--DUMP grouped_counts;
STORE grouped_counts INTO '/user/diederik/compare/';
