REGISTER 'kraken-dclass.jar'
REGISTER 'kraken-pig.jar' 
REGISTER 'piggybank.jar'
REGISTER 'geoip-1.2.5.jar'

DEFINE GEO     org.wikimedia.analytics.kraken.pig.GeoIpLookup('countryCode', 'GeoIPCity');
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtract();
DEFINE DCLASS  org.wikimedia.analytics.kraken.pig.UserAgentClassifier(''); 
DEFINE TO_DAY  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd');

LOG_FIELDS     = LOAD '/wmf/raw/webrequest/webrequest-wikipedia-mobile/2013-02-01*' USING PigStorage('\t') AS (
                                                kafka_byte_offset:long,
                                                hostname:chararray,
                                                udplog_sequence:chararray,
                                                timestamp:chararray,
                                                request_time:chararray,
                                                remote_addr:chararray,
                                                http_status:chararray,
                                                bytes_sent:chararray,
                                                request_method:chararray,
                                                uri:chararray,
                                                proxy_host:chararray,
                                                content_type:chararray,
                                                referer:chararray,
                                                x_forwarded_for:chararray,
                                                user_agent:chararray,
                                                http_language:chararray,
                                                x_cs:chararray
);

LOG_FIELDS     = FILTER LOG_FIELDS BY (request_method MATCHES '(GET|get)');
LOG_FIELDS     = FILTER LOG_FIELDS BY (content_type MATCHES '.*(text\\/html).*' OR (content_type == '-'));

LOG_FIELDS     = FOREACH LOG_FIELDS GENERATE 
                                        TO_DAY(timestamp) AS day,
                                        FLATTEN(GEO(remote_addr)) AS country,
                                        FLATTEN(DCLASS(user_agent)) AS (device:chararray, vendor:chararray, model:chararray, is_wireless:chararray, is_tablet:chararray);


COUNT          = FOREACH (GROUP LOG_FIELDS BY (day, country, device, vendor, model) PARALLEL 10) GENERATE FLATTEN(group), COUNT(LOG_FIELDS.country) as num, COUNT(LOG_FIELDS.(device,vendor,model)), COUNT(LOG_FIELDS.is_tablet);
COUNT          = ORDER COUNT BY day, country, device, vendor, model;

DUMP COUNT;
--STORE COUNT into '$output';
