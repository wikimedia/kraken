-- Deduplicates webrequest logs by grouping on
-- hostname,sequence,timestamp and bytes_sent.
-- We cannot use 'DISTINCT' since the Kafka Byte Offset may
-- be different if the duplicates existed in Kafka.
REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'

%default date_format 'yyyy-MM-dd_hh.mm.ss';

DEFINE DATE_CONVERT      org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', '$date_format');

IMPORT 'include/load_webrequest.pig';
LOG_FIELDS = LOAD_WEBREQUEST('$input');
LOG_FIELDS = FOREACH LOG_FIELDS GENERATE
  kafka_byte_offset,
  hostname,
  sequence,
  timestamp,
  request_time,
  remote_addr,
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
  x_cs,
  DATE_CONVERT(timestamp)      AS dt:chararray;


LOG_FIELDS = FILTER LOG_FIELDS BY (dt >= '$lower_time_bound' AND dt < '$upper_time_bound');

GROUPED = GROUP LOG_FIELDS BY (hostname,sequence,timestamp,bytes_sent,uri);

FILTERED =  FOREACH GROUPED {
       LINE = LIMIT LOG_FIELDS 1;
       GENERATE FLATTEN(LINE);
};

FILTERED = FOREACH FILTERED GENERATE 
  kafka_byte_offset,
  hostname,
  sequence,
  timestamp,
  request_time,
  remote_addr,
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
  x_cs;

STORE FILTERED INTO '$output';
