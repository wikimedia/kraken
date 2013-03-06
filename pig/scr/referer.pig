LOG_FIELDS       = LOAD '$input' USING PigStorage(' ') AS (hostname:chararray, udplog_sequence:chararray, timestamp:chararray, request_time:chararray, remote_addr:chararray, http_status:chararray, bytes_sent:chararray, request_method:chararray, uri:chararray, proxy_host:chararray, content_type:chararray, referer:chararray, x_forwarded_for:chararray, user_agent:chararray);
LOG_FIELDS       = FILTER LOG_FIELDS BY (uri matches '.*BannerController.*');
REFERER          = FOREACH LOG_FIELDS GENERATE referer;
COUNT            = FOREACH (GROUP REFERER BY $0 PARALLEL 7) GENERATE $0, COUNT($1) as num;
COUNT_SORTED     = ORDER COUNT BY num DESC;
--DUMP COUNT_SORTED;
STORE COUNT_SORTED into '$output';
