REGISTER 'kraken-dclass-0.0.1-SNAPSHOT.jar'
REGISTER 'kraken-pig-0.0.1-SNAPSHOT.jar'
REGISTER 'piggybank.jar'
REGISTER 'geoip-1.2.5.jar'

-- import LOAD_WEBREQUEST macro to load in webrequest log fields.
IMPORT 'include/load_webrequest.pig';

DEFINE EXTRACT             org.apache.pig.piggybank.evaluation.string.RegexExtract();
DEFINE GEOCODE_COUNTRY     org.wikimedia.analytics.kraken.pig.GeoIpLookup('countryCode', 'GeoIPCity');
DEFINE DCLASS              org.wikimedia.analytics.kraken.pig.UserAgentClassifier(''); 
DEFINE TO_DAY              org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd_HH:MM:00');


LOG_FIELDS     = LOAD_WEBREQUEST('$input');

LOG_FIELDS     = FILTER LOG_FIELDS BY (request_method MATCHES '(GET|get)');
LOG_FIELDS     = FILTER LOG_FIELDS BY (content_type MATCHES '.*(text\\/html).*' OR (content_type == '-'));

LOG_FIELDS     = FOREACH LOG_FIELDS GENERATE 
                    TO_DAY(timestamp) AS day,
                    FLATTEN(GEOCODE_COUNTRY(remote_addr)) AS country,
                    FLATTEN(DCLASS(user_agent)) AS 
                       (device:chararray,
                        vendor:chararray,
                        model:chararray,
                        is_wireless:chararray,
                        is_tablet:chararray);

COUNT          = FOREACH (GROUP LOG_FIELDS BY (day, country, device, vendor) PARALLEL 10) GENERATE FLATTEN(group), COUNT(LOG_FIELDS.country) as num;
COUNT          = ORDER COUNT BY day, country, device, vendor;

--DUMP COUNT;
STORE COUNT into '$output';