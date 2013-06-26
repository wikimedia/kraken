REGISTER 'geoip-1.2.9-patch-2-SNAPSHOT.jar'
REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'

-- Script Parameters
--      Pass via `-p param_name=param_value`. Ex: pig myscript.pig -p date_bucket_regex=2013-03-24_00
-- Required:
--      input                                   -- Input data paths.
--      output                                  -- Output path for carrier data.

-- %default date_bucket_format 'yyyy-MM-dd';       -- Format applied to timestamps for aggregation into buckets. Default: daily.
-- %default date_bucket_regex '.*';                -- Regex used to filter the formatted date_buckets; must match whole line. Default: no filtering.

DEFINE DATE_BUCKET  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd_HH');
DEFINE GEO          org.wikimedia.analytics.kraken.pig.GeoIpLookupEvalFunc('countryCode', 'GeoIPCity');
DEFINE ZERO         org.wikimedia.analytics.kraken.pig.ZeroFilterFunc('legacy');
DEFINE PAGEVIEW_TYPE org.wikimedia.analytics.kraken.pig.PageViewEvalFunc();


--IMPORT 'include/load_webrequest-no-offset.pig'; -- See include/load_webrequest.pig
--log_fields = LOAD_WEBREQUEST('$input');

log_fields    = LOAD '$input' USING PigStorage('\t', '-tagsource') AS (
    filename:chararray,
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

/*
    Only keep log lines for which all of the following is true:
     - X-CS/X-Analytics header is present
     - timestamp matches $date_bucket_format
     - host matches *.wikipedia.org
     - request is a pageview
    
    Note: We push the filter up as far as possible to minimize the data we compute on.
*/
log_fields = FILTER log_fields
    BY ZERO(uri, filename);    
--      (x_cs IS NOT NULL) AND (x_cs != '') AND (x_cs != '-')
--      AND (DATE_BUCKET(timestamp) MATCHES 'yyyy-MM-dd_HH')
--        AND (uri MATCHES '.*\\.org/wiki/.*')
--   );

log_fields = FOREACH log_fields
    GENERATE
        DATE_BUCKET(timestamp)      AS date_bucket:chararray,
        FLATTEN(GEO(remote_addr))   AS (country:chararray);
--        FLATTEN(ZERO(x_cs))         AS (carrier:chararray, carrier_iso:chararray);


carrier_count = FOREACH (GROUP log_fields BY (date_bucket, country))
    GENERATE FLATTEN($0), COUNT($1) AS num:int;

-- save results into $output
--STORE COUNT INTO '$output' USING PigStorage();
DUMP carrier_count;
