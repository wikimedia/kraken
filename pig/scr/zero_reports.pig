REGISTER 'piggybank.jar'
REGISTER 'kraken.jar'
REGISTER 'geoip-1.2.5.jar'
SET default_parallism 7;

DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtract();
DEFINE PARSE org.wikimedia.analytics.kraken.pig.ParseWikiUrl();
DEFINE GEO org.wikimedia.analytics.kraken.pig.GetCountryCode('GeoIP.dat');
DEFINE TO_MONTH org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM');
DEFINE TO_MONTH_MS org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss.SSS', 'yyyy-MM');
DEFINE HAS_MS org.wikimedia.analytics.kraken.pig.RegexMatch('.*\\.[0-9]{3}');

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
LOG_FIELDS     = FILTER LOG_FIELDS BY content_type == 'text/html' OR (content_type == '-');

PARSED    = FOREACH LOG_FIELDS GENERATE
		    FLATTEN(PARSE(uri)) AS (language_code:chararray, isMobile:chararray, domain:chararray),
		    GEO(remote_addr) AS country,
		    (HAS_MS(timestamp) ? TO_MONTH_MS(timestamp) : TO_MONTH(timestamp)) AS month;

FILTERED    = FILTER PARSED BY (domain eq 'wikipedia.org');
        
GROUPED     = GROUP FILTERED BY (month, language_code, isMobile, country);

COUNT    = FOREACH GROUPED GENERATE
            FLATTEN(group) AS (month, language_code, isMobile, country),
            COUNT_STAR(FILTERED) PARALLEL 7;

STORE COUNT into '$output';
