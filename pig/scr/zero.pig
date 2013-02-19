register 'kraken-pig-0.0.1-SNAPSHOT.jar'
register 'geoip-1.2.5.jar'

DEFINE GEO     org.wikimedia.analytics.kraken.pig.GeoIpLookup('countryCode', 'GeoIPCity');
DEFINE TO_DAY  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd');
DEFINE ZERO  org.wikimedia.analytics.kraken.pig.Zero();

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

LOG_FIELDS = FILTER LOG_FIELDS BY (x_cs != '-');



LOG_FIELDS     = FOREACH LOG_FIELDS GENERATE 
                                        TO_DAY(timestamp) AS day,
                                        FLATTEN(GEO(remote_addr)) AS country,
                                        FLATTEN(ZERO(x_cs)) AS carrier:chararray;


COUNT          = FOREACH (GROUP LOG_FIELDS BY (day, carrier, country) PARALLEL 10) GENERATE FLATTEN(group), COUNT(LOG_FIELDS.carrier) as num;
COUNT          = ORDER COUNT BY day, carrier, country;


DUMP COUNT;
--STORE COUNT into '$output';
