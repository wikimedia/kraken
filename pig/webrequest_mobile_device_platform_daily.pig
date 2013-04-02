/* For testing:
set default_parallel 2;
*/
SET default_parallel 10;

/* Set by oozie or CLI flag (e.g., -p day_regex='2013-01-\d\d') */
%default day_regex '.*';

/* For testing:
REGISTER 'hdfs:///libs/kraken-0.0.2/kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'hdfs:///libs/kraken-0.0.2/kraken-dclass-0.0.2-SNAPSHOT.jar'
REGISTER 'hdfs:///libs/kraken-0.0.2/kraken-pig-0.0.2-SNAPSHOT.jar'
*/
REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'kraken-dclass-0.0.2-SNAPSHOT.jar'
REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'

DEFINE GET_DAY      org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd');
DEFINE DCLASS       org.wikimedia.analytics.kraken.pig.UserAgentClassifier();
DEFINE IS_PAGEVIEW  org.wikimedia.analytics.kraken.pig.PageViewFilterFunc();

IMPORT 'include/load_webrequest.pig';
/* For testing:
IMPORT 'hdfs:///libs/kraken/pig/include/load_webrequest.pig';
*/

/* For testing:
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/2013-01-31_*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/2013-01-31_22.30*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/2013-03-22_16.30*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/2013-03-25_22.30*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/2013-03-25_16.30*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/2013-04-01_16.45*,hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/2013-04-01_17.*');
log_fields = LOAD_WEBREQUEST('hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/2013-04-01*');
log_fields = LOAD_WEBREQUEST('pig.sample.webrequest.wikipedia.mobile*');
*/
log_fields = LOAD_WEBREQUEST('$input');

/* For testing:
matching_log_fields = FILTER log_fields BY (
    (GET_DAY(timestamp) MATCHES '.*')
    AND IS_PAGEVIEW(uri, referer, user_agent, http_status, remote_addr, content_type, request_method)
);
matching_log_fields = FILTER log_fields BY (
    (GET_DAY(timestamp) MATCHES '.*')
);
platform_info = FOREACH matching_log_fields
    GENERATE
        IS_PAGEVIEW(uri, referer, user_agent, http_status, remote_addr, content_type, request_method) AS pageview,
        uri, http_status, referer, request_method, content_type,
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
platform_info = FILTER platform_info BY wmf_mobile_app == 'Android' AND request_method == 'GET';
platform_info = FOREACH platform_info GENERATE pageview, http_status, content_type, uri;
platform_info = ORDER platform_info BY pageview;
-- referer is always '-' for Android
*/
matching_log_fields = FILTER log_fields BY (
    (GET_DAY(timestamp) MATCHES '$day_regex')
    AND IS_PAGEVIEW(uri, referer, user_agent, http_status, remote_addr, content_type, request_method)
);

platform_info = FOREACH matching_log_fields
    GENERATE
        GET_DAY(timestamp) AS day:chararray,
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
platform_info = FOREACH platform_info GENERATE day, wmf_mobile_app;

platform_info_group = GROUP platform_info BY (day, wmf_mobile_app);
platform_info_count = FOREACH platform_info_group GENERATE FLATTEN(group), COUNT(platform_info);
ordered_platform_info_count = ORDER platform_info_count BY day;

STORE ordered_platform_info_count INTO '$output' USING PigStorage();
