REGISTER 'kraken.jar'
REGISTER 'piggybank.jar'

SET default_parallel 20;

DEFINE TO_DAY_HOUR  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd HH');
DEFINE EXTRACT      org.apache.pig.builtin.REGEX_EXTRACT_ALL();

LOG_FIELDS     = LOAD '$input' AS (kafka_byte_offset:long, hostname:chararray, sequence:long, timestamp:chararray, request_time:chararray, remote_addr:chararray, http_status:chararray, bytes_sent:chararray, request_method:chararray, uri:chararray, proxy_host:chararray, content_type:chararray, referer:chararray, x_forwarded_for:chararray, user_agent:chararray);

LOG_FIELDS  = FOREACH LOG_FIELDS GENERATE
 hostname, 
 sequence,
 TO_DAY_HOUR(timestamp) AS day_hour:chararray;
 
LOG_FIELDS       = GROUP LOG_FIELDS BY (hostname,day_hour);
LOG_LOSS       = FOREACH LOG_FIELDS GENERATE 
  group.hostname as hostname,
  group.day_hour as day_hour,
  MIN(LOG_FIELDS.sequence) as sequence_min:long, 
  MAX(LOG_FIELDS.sequence) as sequence_max:long,
  COUNT(LOG_FIELDS.sequence) as sequence_count_actual:long;

LOG_LOSS = FOREACH LOG_LOSS GENERATE
  day_hour, hostname, sequence_min, sequence_max, sequence_count_actual,
  -- max - min + 1 == number of lines there should be in this period.
  (sequence_max - sequence_min + 1) as sequence_count_should_be:long,
  -- (should be - actual) / should_be * 100.0 == loss % 
  (((sequence_max - sequence_min + 1 - sequence_count_actual) / (sequence_max - sequence_min + 1)) * 100.0) as percent_loss:float;

-- order LOG_LOSS by time
LOG_LOSS = ORDER LOG_LOSS BY day_hour, hostname;

-- save results into $output
STORE LOG_LOSS INTO '$output' USING PigStorage();