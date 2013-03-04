REGISTER 'kraken-pig-0.0.1-SNAPSHOT.jar'
REGISTER 'kraken-generic-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
-- import LOAD_WEBREQUEST macro to load in webrequest log fields.
IMPORT 'include/load_webrequest.pig';

-- setting this to 10 because we have 10 worker nodes
SET default_parallel 10;

DEFINE PAGEVIEW     org.wikimedia.analytics.kraken.pig.PageViewFilterFunc();
DEFINE PARSE        org.wikimedia.analytics.kraken.pig.ParseWikiUrl();
DEFINE EXTRACT_ALL  org.apache.pig.builtin.REGEX_EXTRACT_ALL();

LOG_FIELDS     = LOAD_WEBREQUEST('$input');

LOG_FIELDS  = FOREACH LOG_FIELDS GENERATE
 uri,
 referer,
 user_agent,
 http_status,
 content_type,
 remote_addr,
 request_method,
 accept_language;

-- only compute stats for hours that match $hour_regex;
-- LOG_FIELDS    = FILTER LOG_FIELDS BY day_hour MATCHES '$hour_regex';


LOG_FIELDS    = FILTER LOG_FIELDS BY PAGEVIEW(uri, referer, user_agent, http_status, remote_addr, content_type, request_method);

LOG_FIELDS  = FOREACH LOG_FIELDS GENERATE
 FLATTEN(PARSE(uri)) AS (language_code:chararray, isMobile:chararray, domain:chararray),
 FLATTEN(REGEX_EXTRACT(accept_language, '-|[a-z]{2}-[a-zA-Z]{2}',0 )) as user_language:chararray;

LOG_FIELDS = FOREACH LOG_FIELDS GENERATE
    language_code,
    isMobile,
     domain,
     FLATTEN(LOWER($3)) as user_language;

COUNT          = FOREACH (GROUP LOG_FIELDS BY (domain, language_code, user_language)) GENERATE FLATTEN(group), COUNT(LOG_FIELDS.user_language) as num;
COUNT          = ORDER COUNT BY domain, language_code, user_language;

-- save results into $output
STORE COUNT INTO '$output' USING PigStorage();
--DUMP COUNT;
