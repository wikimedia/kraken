REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'

-- Script Parameters
--      Pass via `-p param_name=param_value`. Ex: pig myscript.pig -p input=my-input-file
-- Required:
--      input                                   -- Input data paths.
--      output                                  -- Output path for country data.

DEFINE GEO          org.wikimedia.analytics.kraken.pig.GeoIpLookupEvalFunc('countryCode', 'GeoIPCity');

-- loading the input
squid_log = LOAD '$input' AS (
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

-- adding geolocation
squid_log_geocoded = FOREACH squid_log GENERATE
    *,
    FLATTEN(GEO(remote_addr, x_forwarded_for)) AS country:chararray
  ;

-- filtering lines without country information. Either those lines contained
-- invalid IP addresses or local ipaddresses like 10.0.0.0/8)
squid_log_geocoded_non_local = FILTER squid_log_geocoded BY country is not null;

-- adding kafka_byte_offset, and stitching country to remote_addr
kafka_log = FOREACH squid_log_geocoded_non_local GENERATE
    0 AS kafka_byte_offset:double,
    hostname,
    sequence,
    timestamp,
    request_time,
    CONCAT(remote_addr, CONCAT('|', country)),
    http_status,
    bytes_sent,
    request_method,
    uri,
    proxy_host,
    content_type,
    referer,
    x_forwarded_for,
    user_agent,
    accept_language,
    x_cs
  ;

STORE kafka_log INTO '$output' USING PigStorage();
