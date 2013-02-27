REGISTER 'kraken.jar'

-- import LOAD_WEBREQUEST macro to load in webrequest log fields.
IMPORT 'include/load_webrequest.pig';

-- setting this to 10 to use all datanodes
SET default_parallel 10;

DEFINE TO_HOUR      org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd_HH');
DEFINE EXTRACT      org.apache.pig.builtin.REGEX_EXTRACT_ALL();

-- Set this as a regular expression to filter for desired timestamps.
-- E.g. if you want to only compute stats for a given hour, then
--   -p hour_regex='2013-01-02_15'.
-- This will make your output only contain values for 15:00 on Jan 2.
-- If you want hourly output for an entire day, then:
--   -p hour_regex='2013-01-02_\d\d'
-- should do just fine.
-- The default is to match all hour timestamps.
%default hour_regex '.*';

LOG_FIELDS     = LOAD_WEBREQUEST('$input');

LOG_FIELDS  = FOREACH LOG_FIELDS GENERATE
 hostname, 
 sequence,
 TO_HOUR(timestamp) AS day_hour:chararray;

-- only compute stats for hours that match $hour_regex;
LOG_FIELDS    = FILTER LOG_FIELDS BY day_hour MATCHES '$hour_regex';


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
  (sequence_max - sequence_min + 1) as sequence_count_should_be:long;

LOG_LOSS = FOREACH LOG_LOSS GENERATE
  day_hour, hostname, sequence_min, sequence_max, sequence_count_actual, sequence_count_should_be,
  -- ((should_be - actual) / should_be) * 100.0 == percent loss
  ((((float)sequence_count_should_be - (float)sequence_count_actual) / (float)sequence_count_should_be) * 100.0) as percent_loss:float;

-- order LOG_LOSS by time
LOG_LOSS = ORDER LOG_LOSS BY day_hour, hostname;

-- save results into $output
STORE LOG_LOSS INTO '$output' USING PigStorage();
