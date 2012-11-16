REGISTER 'piggybank.jar'
REGISTER 'kraken.jar'
--REGISTER 'geoip-1.2.5.jar'

DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtract();
DEFINE PARSE org.wikimedia.analytics.kraken.pig.ParseWikiUrlER();
--DEFINE GEO org.wikimedia.analytics.kraken.pig.GetCountryCode('GeoIP.dat');
DEFINE TO_DAY org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd');
DEFINE TO_DAY_MS org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss.SSS', 'yyyy-MM-dd');
DEFINE HAS_MS org.wikimedia.analytics.kraken.pig.RegexMatch('.*\\.[0-9]{3}');
DEFINE PigStorageWithInputPath org.wikimedia.analytics.kraken.pig.PigStorageWithInputPath();

LOG_FIELDS     = LOAD 'small' USING PigStorageWithInputPath() AS (
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
    user_agent,
    filename:chararray);

LOG_FIELDS     = FILTER LOG_FIELDS BY (request_method MATCHES '(GET|get)');
LOG_FIELDS     = FILTER LOG_FIELDS BY content_type == 'text/html' OR (content_type == '-');

PARSED    = FOREACH LOG_FIELDS GENERATE
          		    FLATTEN(PARSE(uri)) AS (language_code:chararray, version:chararray, project:chararray),
--		              GEO(remote_addr) AS country,
		              (HAS_MS(timestamp) ? TO_DAY_MS(timestamp) : TO_DAY(timestamp)) AS date,
                      EXTRACT(filename, 'file\\:.*/zero-(.*)\\.log-[0-9]{8}\\.gz', 1) as provider;

-- GROUPED        = GROUP PARSED BY (provider, date, language_code, project, version, country);
GROUPED        = GROUP PARSED BY (provider, date, language_code, project, version);

COUNT    = FOREACH GROUPED GENERATE
--            FLATTEN(group) AS (provider, date, language_code, project, version, country),
            FLATTEN(group) AS (provider, date, language_code, project, version),
            COUNT_STAR(PARSED);

STORE COUNT into 'counts';

