REGISTER 'kraken-pig-0.0.1-SNAPSHOT.jar'
REGISTER 'kraken-generic-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'geoip-1.2.5.jar'

-- import LOAD_WEBREQUEST macro to load in webrequest log fields.
IMPORT 'include/load_webrequest.pig';

-- setting this to 10 because we have 10 worker nodes
SET default_parallel 10;

DEFINE TO_HOUR      org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd_HH');
DEFINE EXTRACT      org.apache.pig.builtin.REGEX_EXTRACT_ALL();
DEFINE ZERO         org.wikimedia.analytics.kraken.pig.Zero();
DEFINE GEO          org.wikimedia.analytics.kraken.pig.GeoIpLookup('countryCode', 'GeoIPCity');

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

-- only keep log lines which have an X-CS header set
LOG_FIELDS    = FILTER LOG_FIELDS BY (x_cs != '-');

LOG_FIELDS  = FOREACH LOG_FIELDS GENERATE
 x_cs,
 remote_addr,
 TO_HOUR(timestamp) AS day_hour:chararray;

-- only compute stats for hours that match $hour_regex;
LOG_FIELDS    = FILTER LOG_FIELDS BY day_hour MATCHES '$hour_regex';

LOG_FIELDS  = FOREACH LOG_FIELDS GENERATE
 day_hour,
 FLATTEN(GEO(remote_addr)) AS country,
 FLATTEN(ZERO(x_cs)) AS carrier:chararray;

COUNT          = FOREACH (GROUP LOG_FIELDS BY (day_hour, carrier, country)) GENERATE FLATTEN(group), COUNT(LOG_FIELDS.carrier) as num;
COUNT          = ORDER COUNT BY day_hour, carrier, country;

-- save results into $output
STORE COUNT INTO '$output' USING PigStorage();
