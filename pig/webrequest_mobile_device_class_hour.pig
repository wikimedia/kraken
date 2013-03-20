REGISTER 'geoip-1.2.9-patch-2-SNAPSHOT.jar'
REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'kraken-dclass-0.0.2-SNAPSHOT.jar'
REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'

SET default_parallel 10;

DEFINE TO_HOUR      org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd_HH');
DEFINE GEO          org.wikimedia.analytics.kraken.pig.GeoIpLookupEvalFunc('countryCode', 'GeoIPCity');
DEFINE DCLASS       org.wikimedia.analytics.kraken.pig.UserAgentClassifier();
DEFINE IS_PAGEVIEW  org.wikimedia.analytics.kraken.pig.PageViewFilterFunc();

-- Set by oozie or CLI flag (e.g., -p hour_regex='2013-01-02_\d\d')
%default hour_regex '.*';

IMPORT 'include/load_webrequest.pig'; -- See include/load_webrequest.pig
log_fields = LOAD_WEBREQUEST('$input');

log_fields = FILTER log_fields
    BY (    (TO_HOUR(timestamp) MATCHES '$hour_regex')
        AND IS_PAGEVIEW(uri, referer, user_agent, http_status, remote_addr, content_type, request_method)
    );

device_info = FOREACH log_fields
    GENERATE
        TO_HOUR(timestamp)          AS day_hour:chararray,
        FLATTEN(GEO(remote_addr))   AS (country:chararray),
        FLATTEN(DCLASS(user_agent)) AS (
            vendor:chararray,
            device_os:chararray,
            device_os_version:chararray,
            has_javascript:boolean,
            is_wireless:boolean,
            is_tablet:boolean,
            width:int,
            height:int,
            wikimedia_app:chararray,
            apple_info:chararray )
    ;

device_info = FOREACH device_info
    GENERATE day_hour, country, (is_wireless ? 'handheld' : (is_tablet ? 'tablet' : 'desktop')) as device_class:chararray;
device_info_count = FOREACH (GROUP device_info BY ALL)
    GENERATE FLATTEN($0), COUNT($1) AS num:int;
device_info_count = ORDER device_info_count BY day_hour, country;

STORE device_info_count INTO '$output' USING PigStorage();
