-- Usage:
--   import 'include/load_webrequest';
--   LOG_FIELDS = LOAD_WEBREQUEST('/path/to/logs');
--
-- ToDo:
--   Rename x_cs to x_analytics.

DEFINE LOAD_WEBREQUEST(input_path) RETURNS WEBREQUEST_FIELDS {
  $WEBREQUEST_FIELDS = LOAD '$input_path' AS (
    kafka_byte_offset:double, -- TODO: test whether truncation is avoided as long
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
};



-- Usage:
--   import 'include/load_webrequest';
--   LOG_FIELDS = LOAD_WEBREQUEST_GEOCODED('/path/to/geocoded/anonymized/logs');
-- Note:  Only use this macro on pre-geocoded and anonymized webrequest data.
--
DEFINE EXTRACT_ALL    org.apache.pig.builtin.REGEX_EXTRACT_ALL();
DEFINE LOAD_WEBREQUEST_GEOCODED(input_path) RETURNS WEBREQUEST_FIELDS {
  FIELDS = LOAD '$input_path' AS (
    kafka_byte_offset:double, -- TODO: test whether truncation is avoided as long
    hostname:chararray,
    sequence:long,
    timestamp:chararray,
    request_time:chararray,
    remote_addr_country:chararray,
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
    x_analytics:chararray
  );

  $WEBREQUEST_FIELDS = FOREACH FIELDS GENERATE
    kafka_byte_offset,
    hostname,
    sequence,
    timestamp,
    request_time,
    -- remote_addr_country will look like <ip_address>|<country_code>.
    -- Here we extract it to two sepearte fields: remote_addr and country.
    FLATTEN(EXTRACT_ALL(remote_addr_country, '(.+)\\|([\\w-]+)')) AS (remote_addr:chararray, country:chararray),
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
    x_analytics;
};
