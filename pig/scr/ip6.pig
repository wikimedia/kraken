REGISTER 'kraken.jar'

LOG_FIELDS               = LOAD '$input' USING PigStorage(' ') AS (hostname:chararray, udplog_sequence:chararray, timestamp:chararray, request_time:chararray, remote_addr:chararray, http_status:chararray, bytes_sent:chararray, request_method:chararray, uri:chararray, proxy_host:chararray, content_type:chararray, referer:chararray, x_forwarded_for:chararray, user_agent:chararray);
DEFINE isValid4 org.wikimedia.analytics.kraken.pig.isValidIPv4Address();
DEFINE isValid6 org.wikimedia.analytics.kraken.pig.isValidIPv6Address();

ipType = FOREACH LOG_FIELDS GENERATE remote_addr, isValid6(remote_addr) AS ipType;
DUMP ipType;
