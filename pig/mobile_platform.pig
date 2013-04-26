REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'kraken-dclass-0.0.2-SNAPSHOT.jar'
REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'

-- Script Parameters: pass via -p param_name=param_value, ex: -p date_bucket_regex=2013-03-24
%default date_bucket_format 'yyyy-MM-dd';       -- Format applied to timestamps for aggregation into buckets. Default: Daily.
%default date_bucket_regex '.*';                -- Regex used to filter the formatted date_buckets; must match whole line. Default: no filtering.

DEFINE DATE_BUCKET  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', '$date_bucket_format');
DEFINE DCLASS       org.wikimedia.analytics.kraken.pig.UserAgentClassifier();
DEFINE IS_PAGEVIEW  org.wikimedia.analytics.kraken.pig.PageViewFilterFunc();

IMPORT 'include/load_webrequest.pig';

/* For testing:
************* Unsampled *************
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/dt=2013-01-31_*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/dt=2013-01-31_22.30*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/dt=2013-03-22_16.30*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/dt=2013-03-25_22.30*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/dt=2013-03-25_16.30*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/dt=2013-04-01_16.45*,hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/dt=2013-04-01_17.*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/dt=2013-04-01*');

************* Sampled ***************
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-all-sampled-1000/2013-04-15_12*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-all-sampled-1000/2013-04-15*');

************* Local *****************
log_fields = LOAD_WEBREQUEST('pig.sample.webrequest.wikipedia.mobile*');
*/
log_fields = LOAD_WEBREQUEST('$input');

log_fields = FOREACH log_fields
    GENERATE
        DATE_BUCKET(timestamp) as date_bucket,
        FLATTEN(STRSPLIT(remote_addr,'\\|')) as (ip_addr:chararray, country_code:chararray),
        uri, referer, user_agent, http_status, content_type, request_method
    ;

/* For testing:
matching_log_fields = FILTER log_fields BY (
    (day MATCHES '.*')
    AND IS_PAGEVIEW(uri, referer, user_agent, http_status, ip_addr, content_type, request_method)
);
NOTE: the ip_addr field is anonymized.  During anonymization, internal IP addresses can be made external and vice versa.
TODO: investigate more carefully whether the anonymization coupled with "IS_PAGEVIEW" changes the results significantly
*/
matching_log_fields = FILTER log_fields BY (
    (date_bucket MATCHES '$date_bucket_regex')
    AND IS_PAGEVIEW(uri, referer, user_agent, http_status, ip_addr, content_type, request_method)
);

platform_info = FOREACH matching_log_fields
    GENERATE
        date_bucket,
        FLATTEN(DCLASS(REPLACE(user_agent, '%20', ' '))) AS (
            vendor:chararray,
            model:chararray,
            device_os:chararray,
            device_os_version:chararray,
            device_class:chararray,
            browser:chararray,
            browser_version:chararray,
            wmf_mobile_app:chararray,
            has_javascript:boolean,
            display_dimensions:chararray,
            input_device:chararray
        )
    ;
platform_info = FILTER platform_info BY wmf_mobile_app is not null;
platform_info = FOREACH platform_info GENERATE date_bucket, wmf_mobile_app;

platform_info_group = GROUP platform_info BY (date_bucket, wmf_mobile_app);
platform_info_count = FOREACH platform_info_group GENERATE FLATTEN(group), COUNT(platform_info) * 1000;
ordered_platform_info_count = ORDER platform_info_count BY date_bucket;

STORE ordered_platform_info_count INTO '$output' USING PigStorage();


/*
-- run with IS_PAGEVIEW
(BlackBerry PlayBook,   110000)
(iOS,                   7025000)
(Android,               5630000)
(Windows 8,             16285000)
(Firefox OS,            1000)

-- run without IS_PAGEVIEW
(BlackBerry PlayBook,   1097000)
(iOS,                   52948000)
(Android,               15128000)
(Windows 8,             159034000)
(Firefox OS,            2000)

-- run with more firm Windows 8 UA string
(BlackBerry PlayBook,   110000)
(iOS,                   7025000)
(Android,               5630000)
(Windows 8,             52000)
(Firefox OS,            1000)
*/
