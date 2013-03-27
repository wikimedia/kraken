REGISTER 'piggybank.jar';
REGISTER 'geoip-1.2.9-patch-2-SNAPSHOT.jar'
REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'kraken-dclass-0.0.2-SNAPSHOT.jar'
REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'

-- Script Parameters: pass via -p param_name=param_value, ex: -p date_bucket_regex=2013-03-24_00
%default session_length '30m';                  -- Duration of inactivity that bounds a session. Default: 30 minutes.
%default date_bucket_format 'yyyy-MM-dd_HH';    -- Format applied to timestamps for aggregation into buckets. Default: hourly.
%default date_bucket_regex '.*';                -- Regex used to filter the formatted date_buckets; must match whole line. Default: no filtering.

DEFINE DATE_BUCKET  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', '$date_bucket_format');
DEFINE IS_PAGEVIEW  org.wikimedia.analytics.kraken.pig.PageViewFilterFunc();
DEFINE ISOToUnix    org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE Sessionize   datafu.pig.sessions.Sessionize('$session_length');
DEFINE MD5          datafu.pig.hash.MD5Base64();
DEFINE First        datafu.pig.bags.FirstTupleFromBag();
DEFINE Last         org.apache.pig.piggybank.evaluation.ExtremalTupleByNthField('1', 'desc');

IMPORT 'include/load_webrequest.pig'; -- See include/load_webrequest.pig

log_fields = LOAD_WEBREQUEST('$input');
log_fields = FILTER log_fields
    BY (    (DATE_BUCKET(timestamp) MATCHES '$date_bucket_regex')
        AND IS_PAGEVIEW(uri, referer, user_agent, http_status, remote_addr, content_type, request_method)
    );

views = FOREACH log_fields
    GENERATE
        timestamp,
        MD5(CONCAT(remote_addr, user_agent))    AS visitor_id:chararray,
        uri,
        FLATTEN(GEO(remote_addr))               AS (country:chararray),
        referer,
        x_cs
    ;

sessions = FOREACH (GROUP views BY visitor_id) {
    ordered_views = ORDER views BY timestamp;
    GENERATE FLATTEN(Sessionize(ordered_views)) as (timestamp, visitor_id, uri, country, referer, x_cs, session_id);
};

sessions = FOREACH (GROUP sessions BY (session_id, visitor_id))
    GENERATE
        MIN(sessions.timestamp)     AS session_start:chararray,
        MAX(sessions.timestamp)     AS session_end:chararray,
        group.visitor_id,
        group.session_id,
        '-'                         AS site_mode:chararray,
        COUNT(sessions)             AS pageviews:int,
        0                           AS special_pageviews:int,
        FLATTEN(First(sessions)).(uri, referer)
                                    AS (entry_uri:chararray, entry_referer:chararray),
        FLATTEN(Last(sessions)).uri AS (exit_uri:chararray)
        -- (MAX(sessions.timestamp) - MIN(sessions.timestamp)) as session_length:long
    ;

STORE sessions INTO '$output' USING PigStorage();
