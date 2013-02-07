-- Usage:
--   import 'include/load_webrequest';
--   LOG_FIELDS = LOAD_WEBREQUEST('/path/to/logs');

DEFINE LOAD_WEBREQUEST(input_path) RETURNS WEBREQUEST_FIELDS {
  $WEBREQUEST_FIELDS = LOAD '$input_path' AS (
    kafka_byte_offset:double,
    hostname:chararray,
    sequence:double,
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
