REGISTER 'piggybank.jar'
REGISTER 'joda-time-1.6.jar'
REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'

-- Setting this to 4 for now, since we are
-- only importing logs from the 4 mobile varnish hosts.
-- There will only be 4 unique hostname keys, so we only
-- need 4 reducers.
SET default_parallel 4;

-- Script Parameters: pass via -p param_name=param_value, ex: -p date_bucket_regex=2013-03-24_00
%default date_bucket_format 'yyyy-MM-dd_HH';    -- Format applied to timestamps for aggregation into buckets. Default: hourly.
%default date_bucket_regex '.*';                -- Regex used to filter the formatted date_buckets; must match whole line. Default: no filtering.

DEFINE ToDateBucket  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', '$date_bucket_format');
DEFINE ISOToHour     org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToHour();

IMPORT 'include/load_webrequest.pig';
LOG_FIELDS = LOAD_WEBREQUEST('$input');

LOG_FIELDS  = FOREACH LOG_FIELDS GENERATE
 ToDateBucket(timestamp) AS date_bucket:chararray,
 ISOToHour(timestamp) AS timestamp:chararray,
 hostname,
 sequence;

-- only compute stats for hours that match $date_bucket_regex;
LOG_FIELDS    = FILTER LOG_FIELDS BY date_bucket MATCHES '$date_bucket_regex';


LOG_FIELDS       = GROUP LOG_FIELDS BY (hostname,timestamp);
LOG_LOSS       = FOREACH LOG_FIELDS GENERATE 
  group.hostname,
  group.timestamp,
  MIN(LOG_FIELDS.sequence) as sequence_min:long, 
  MAX(LOG_FIELDS.sequence) as sequence_max:long,
  COUNT(LOG_FIELDS.sequence) as sequence_count_actual:long;

LOG_LOSS = FOREACH LOG_LOSS GENERATE
  timestamp, hostname, sequence_min, sequence_max, sequence_count_actual,
  -- max - min + 1 == number of lines there should be in this period.
  (sequence_max - sequence_min + 1) as sequence_count_should_be:long;

LOG_LOSS = FOREACH LOG_LOSS GENERATE
  timestamp, hostname, sequence_min, sequence_max, sequence_count_actual, sequence_count_should_be,
  -- ((should_be - actual) / should_be) * 100.0 == percent loss
  ((((float)sequence_count_should_be - (float)sequence_count_actual) / (float)sequence_count_should_be) * 100.0) as percent_loss:float;

-- order LOG_LOSS by time
LOG_LOSS = ORDER LOG_LOSS BY timestamp, hostname;

-- save results into $output
STORE LOG_LOSS INTO '$output' USING PigStorage();
