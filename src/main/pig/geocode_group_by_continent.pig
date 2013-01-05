-- Usage:
-- pig -p input=/hdfs/path/to/access_logs* -p output=/hdfs/path/to/output_directory -f geocode_group_by_continent.pig
REGISTER 'piggybank.jar'
REGISTER 'kraken.jar'
REGISTER 'geoip-1.2.5.jar'
SET default_parallism 7;
 
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtract();
DEFINE GEO     org.wikimedia.analytics.kraken.pig.GeoIpLookup('GeoIPCity.dat');
DEFINE TO_HOUR org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd_HH');
DEFINE MATCH   org.wikimedia.analytics.kraken.pig.RegexMatch();

LOG_FIELDS     = LOAD '$input' USING PigStorage(' ') AS (
  byteoffset_hostname,
  udplog_sequence,
  timestamp:chararray,
  request_time,
  remote_addr:chararray,
  http_status,
  bytes_sent,
  request_method:chararray,
  uri:chararray,
  proxy_host,
  content_type:chararray,
  referer,
  x_forwarded_for,
  user_agent );
 

LOG_FIELDS     = FILTER LOG_FIELDS BY (request_method MATCHES '(GET|get)');
LOG_FIELDS     = FILTER LOG_FIELDS BY (http_status MATCHES '.*(200|304)'); 
LOG_FIELDS     = FOREACH LOG_FIELDS GENERATE (EXTRACT(content_type, 'text/html', 0)) as content_type:chararray, http_status, request_method, timestamp, remote_addr, uri;
LOG_FIELDS     = FILTER LOG_FIELDS BY content_type == 'text/html';
 
 
PARSED    = FOREACH LOG_FIELDS GENERATE
            TO_HOUR(timestamp) AS hour,
            FLATTEN(GEO(remote_addr)) AS (country:chararray, country_code:chararray, region:chararray,  city:chararray,  postal_code:chararray, metro_code:int, continent_code:chararray, continent_name:chararray);

PARSED    = FOREACH PARSED GENERATE hour, continent_name;

GROUPED     = GROUP PARSED BY (hour, continent_name);
 
COUNT       = FOREACH GROUPED GENERATE
              FLATTEN(group) AS (day, continent_name),
              COUNT_STAR($1) as num PARALLEL 1;

--DUMP COUNT;
STORE COUNT INTO '$output' USING PigStorage();