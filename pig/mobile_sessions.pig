REGISTER 'hdfs:///user/oozie/share/lib/pig/joda-time-1.6.jar'
REGISTER 'hdfs:///libs/piggybank.jar';
REGISTER 'hdfs:///libs/datafu-0.0.9.jar';
REGISTER 'hdfs:///libs/geoip-1.2.9-patch-2-SNAPSHOT.jar'
REGISTER 'hdfs:///libs/kraken-0.0.2/kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'hdfs:///libs/kraken-0.0.2/kraken-dclass-0.0.2-SNAPSHOT.jar'
REGISTER 'hdfs:///libs/kraken-0.0.2/kraken-pig-0.0.2-SNAPSHOT.jar'

-- Script Parameters
--      Pass via `-p param_name=param_value`. Ex: pig myscript.pig -p date_bucket_regex=2013-03-24_00
-- Required:
--      input                                   -- Input data paths.
--      output                                  -- Output data path.
-- Optional:
%default session_length     '30m';              -- Duration of inactivity that bounds a session. Default: 30 minutes.
%default date_bucket_format 'yyyy-MM-dd_HH';    -- Format applied to timestamps for aggregation into buckets. Default: hourly.
%default date_bucket_regex  '.*';               -- Regex used to filter the formatted date_buckets; must match whole line. Default: no filtering.

DEFINE ISOToUnix    org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE ToDateBucket org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', '$date_bucket_format');
DEFINE IsPageview   org.wikimedia.analytics.kraken.pig.PageViewFilterFunc();
DEFINE KVToMap      org.wikimedia.analytics.kraken.pig.maps.KVPairsToMap(';', '=');
-- DEFINE GeoIP        org.wikimedia.analytics.kraken.pig.GeoIpLookupEvalFunc('countryCode', 'GeoIPCity');
DEFINE Sessionize   datafu.pig.sessions.Sessionize('$session_length');
DEFINE MD5          datafu.pig.hash.MD5();
DEFINE First        datafu.pig.bags.FirstTupleFromBag();

IMPORT 'hdfs:///libs/kraken/pig/include/load_webrequest.pig';

log_fields = LOAD_WEBREQUEST('$input');
log_fields = FILTER log_fields
    BY (    (remote_addr IS NOT NULL) AND (remote_addr != '')
        AND (user_agent  IS NOT NULL) AND (user_agent != '') AND (user_agent != '-')
        AND (ToDateBucket(timestamp) MATCHES '$date_bucket_regex')
        AND IsPageview(uri, referer, user_agent, http_status, remote_addr, content_type, request_method)
    );

views = FOREACH log_fields {
    ipua = (chararray) CONCAT((chararray)remote_addr, (chararray)user_agent);
    GENERATE
        timestamp,
        ISOToUnix(timestamp)    AS unixtime:long,
        MD5(ipua)               AS visitor_id:chararray,
        uri,
        -- FLATTEN(GeoIP(remote_addr)) AS (country:chararray),
        referer,
        x_cs
    ;
};

sessions = FOREACH (GROUP views BY visitor_id) {
    ordered_views = ORDER views BY unixtime ASC;
    GENERATE FLATTEN(Sessionize(ordered_views)) as (timestamp, unixtime, visitor_id, uri, referer, x_cs, session_id);
};

session_info = FOREACH (GROUP sessions BY (session_id, visitor_id)) {
    sessions_asc = ORDER sessions BY unixtime ASC;
    first_session = LIMIT sessions_asc 1; -- TODO: Can be replaced with First() post-ORDER BY?
    sessions_desc = ORDER sessions BY unixtime DESC;
    last_session = LIMIT sessions_desc 1; -- TODO: Can be replaced with First() post-ORDER BY?
    x_analytics = KVToMap( (chararray) First(first_session, NULL).x_cs );
    special_pages = FILTER sessions BY (uri MATCHES 'https?://[^/]+/wiki/Special:.*');
    
    GENERATE
        FLATTEN(first_session.timestamp)    AS (session_start:chararray),
        FLATTEN(last_session.timestamp)     AS (session_end:chararray),
        group.visitor_id,
        group.session_id,
        x_analytics#'mf-m'                  AS site_mode:chararray,
        COUNT(sessions)                     AS pageviews:int,
        COUNT(special_pages)                AS special_pageviews:int,
        FLATTEN(first_session.uri)          AS (entry_uri:chararray),
        FLATTEN(first_session.referer)      AS (entry_referer:chararray),
        FLATTEN(last_session.uri)           AS (exit_uri:chararray)
        -- ((long)last_session.unixtime - (long)first_session.unixtime) as session_length:long
    ;
};

STORE session_info INTO '$output' USING PigStorage();
