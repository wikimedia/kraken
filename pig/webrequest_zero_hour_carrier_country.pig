REGISTER 'geoip-1.2.9-patch-2-SNAPSHOT.jar'
REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'kraken-dclass-0.0.2-SNAPSHOT.jar'
REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'

-- Script Parameters: pass via -p param_name=param_value, ex: -p date_bucket_regex=2013-03-24_00
%default date_bucket_format 'yyyy-MM-dd_HH';    -- Format applied to timestamps for aggregation into buckets. Default: hourly.
%default date_bucket_regex '.*';                -- Regex used to filter the formatted date_buckets; must match whole line. Default: no filtering.

DEFINE DATE_BUCKET  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', '$date_bucket_format');
DEFINE GEO          org.wikimedia.analytics.kraken.pig.GeoIpLookupEvalFunc('countryCode', 'GeoIPCity');
DEFINE ZERO         org.wikimedia.analytics.kraken.pig.Zero();
DEFINE IS_PAGEVIEW  org.wikimedia.analytics.kraken.pig.PageViewFilterFunc();
DEFINE PAGEVIEW     org.wikimedia.analytics.kraken.pig.PageViewEvalFunc();

IMPORT 'include/load_webrequest.pig'; -- See include/load_webrequest.pig
log_fields = LOAD_WEBREQUEST('$input');

/*
    Only keep log lines for which all of the following is true:
     - X-CS/X-Analytics header is present
     - timestamp matches $date_bucket_format
     - host matches *.wikipedia.org
     - request is a pageview
    
    Note: We push the filter up as far as possible to minimize the data we compute on.
*/
log_fields = FILTER log_fields
    BY (    (x_cs IS NOT NULL) AND (x_cs != '') AND (x_cs != '-')
        AND (x_cs MATCHES '\\d\\d\\d-\\d.*')
        AND (DATE_BUCKET(timestamp) MATCHES '$date_bucket_format')
        AND (uri MATCHES 'https?://([^/]+?\\.)?wikipedia\\.org/.*')
        AND IS_PAGEVIEW(uri, referer, user_agent, http_status, remote_addr, content_type, request_method)
    );

log_fields = FOREACH log_fields
    GENERATE
        DATE_BUCKET(timestamp)      AS date_bucket:chararray,
        FLATTEN(PAGEVIEW(uri, referer, user_agent, http_status, remote_addr, content_type, request_method))
                                    AS (language:chararray, project:chararray, site_version:chararray, article_title:chararray),
        FLATTEN(GEO(remote_addr))   AS (country:chararray),
        FLATTEN(ZERO(x_cs))         AS (carrier:chararray, carrier_iso:chararray)
    ;

country_count = FOREACH (GROUP log_fields BY (date_bucket, language, project, site_version, country))
    GENERATE FLATTEN($0), '-', COUNT($1) AS num:int;
STORE country_count INTO '$output/country' USING PigStorage();

carrier_count = FOREACH (GROUP log_fields BY (date_bucket, language, project, site_version, country, carrier))
    GENERATE FLATTEN($0), COUNT($1) AS num:int;
STORE carrier_count INTO '$output/carrier' USING PigStorage();
