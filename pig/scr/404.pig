REGISTER 'piggybank.jar'
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtract();
LOG_FIELDS     = LOAD '/user/otto/logs/sampled/sampled-1000.log-20120202' USING PigStorage(' ') AS (hostname:chararray, udplog_sequence:chararray, timestamp:chararray, request_time:chararray, remote_addr:chararray, http_status:chararray, bytes_sent:chararray, request_method:chararray, uri:chararray, proxy_host:chararray, content_type:chararray, referer:chararray, x_forwarded_for:chararray, user_agent:chararray);
LOG_FIELDS     = FOREACH LOG_FIELDS GENERATE (EXTRACT(http_status, '404', 0)) as http_status:chararray, referer;

--COUNT          = GROUP LOG_FIELDS BY referer, COUNT(http_status) as num), PARALLEL 7;
--COUNT          = ORDER COUNT BY referer, num;
--STORE COUNT into '$output';
