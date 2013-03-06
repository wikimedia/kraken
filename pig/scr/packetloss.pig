REGISTER 'kraken.jar'
REGISTER 'piggybank.jar'

SET default_parallel 20;

DEFINE TO_HOUR_DAY  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd HH');
DEFINE EXTRACT org.apache.pig.builtin.REGEX_EXTRACT_ALL();

LOG_FIELDS     = LOAD '$input' USING PigStorage(' ') AS (hostname:chararray, udplog_sequence:long, timestamp:chararray, request_time:chararray, remote_addr:chararray, http_status:chararray, bytes_sent:chararray, request_method:chararray, uri:chararray, proxy_host:chararray, content_type:chararray, referer:chararray, x_forwarded_for:chararray, user_agent:chararray);

LOG_FIELDS  = FOREACH LOG_FIELDS GENERATE FLATTEN(EXTRACT(hostname, '\\d+\\s(.+)')) as hostname:chararray, udplog_sequence, TO_HOUR_DAY(timestamp) AS hour_day;

LOG_FIELDS       = GROUP LOG_FIELDS BY (hostname,hour_day);
PACKETLOSS       = FOREACH LOG_FIELDS GENERATE 
		   group.hostname as hostname,		
	   	group.hour_day as hour_day,
                    MAX(LOG_FIELDS.udplog_sequence) as udp_max:long, 
		 MIN(LOG_FIELDS.udplog_sequence) as udp_min:long, 
		COUNT(LOG_FIELDS.udplog_sequence) as udp_count:long;


PACKETLOSS = ORDER PACKETLOSS BY hour_day, hostname;
DUMP PACKETLOSS;
--STORE COUNT into '$output';
