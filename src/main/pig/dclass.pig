-- Usage:
-- pig -p input=/hdfs/path/to/access_logs* -p output=/hdfs/path/to/output_directory -f blog.pig
REGISTER 'piggybank.jar'
REGISTER 'kraken-dclass.jar'
REGISTER 'geoip-1.2.5.jar'
SET default_parallism 7;

DEFINE DCLASS  org.wikimedia.analytics.kraken.pig.UserAgentClassifier(); 
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtract();
DEFINE TO_DAY  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd');
DEFINE MATCH   org.wikimedia.analytics.kraken.pig.RegexMatch();

LOG_FIELDS     = LOAD '/wmf/raw/webrequest/webrequest-wikipedia-mobile/2013-01-20*' USING PigStorage(' ') AS (
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
 

--LOG_FIELDS     = FILTER LOG_FIELDS BY (request_method MATCHES '(GET|get)');
--LOG_FIELDS     = FILTER LOG_FIELDS BY (http_status MATCHES '.*(200|304)'); 
--LOG_FIELDS     = FOREACH LOG_FIELDS GENERATE (EXTRACT(content_type, 'text/html', 0)) as content_type:chararray, http_status, request_method, timestamp, remote_addr, uri;
--LOG_FIELDS     = FILTER LOG_FIELDS BY content_type == 'text/html';
--LOG_FIELDS     = FILTER LOG_FIELDS BY (uri MATCHES '.*aaron.*');
 
PARSED    = FOREACH LOG_FIELDS GENERATE
            TO_DAY(timestamp) AS day,
            FLATTEN(DCLASS(user_agent)) AS device,
            FLATTEN(uri) as uri; 
 
GROUPED     = GROUP PARSED BY (day, device, uri);
 
COUNT       = FOREACH GROUPED GENERATE
              FLATTEN(group) AS (day, device, uri),
              COUNT_STAR($1) as num PARALLEL 10;

DUMP COUNT;
--STORE COUNT INTO '$output' USING PigStorage();
