-- Usage:
-- pig -p input=/hdfs/path/to/access_logs* -p output=/hdfs/path/to/output_directory -f blog.pig
REGISTER 'piggybank.jar'
REGISTER 'kraken.jar'
REGISTER 'geoip-1.2.5.jar'
SET default_parallism 7;
 
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtract();
DEFINE GEO     org.wikimedia.analytics.kraken.pig.GetCountryCode('GeoIP.dat');
DEFINE TO_DAY  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd');
DEFINE MATCH   org.wikimedia.analytics.kraken.pig.RegexMatch();

LOG_FIELDS     = LOAD '$input' USING PigStorage(' ') AS (
  hostname,
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
            TO_DAY(timestamp) AS day,
            FLATTEN(GEO(remote_addr)) AS country,
            FLATTEN(uri) as uri; 
 
GROUPED     = GROUP PARSED BY (day, country, uri);
 
COUNT       = FOREACH GROUPED GENERATE
              FLATTEN(group) AS (day, country, uri),
              COUNT_STAR($1) as num PARALLEL 1;

--DUMP COUNT;
STORE COUNT INTO '$output' USING PigStorage();
