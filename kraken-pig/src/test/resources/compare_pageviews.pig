log_fields    = LOAD 'input' USING PigStorage('\t') AS (
    hostname:chararray,
    sequence:long,
    timestamp:chararray,
    request_time:chararray,
    remote_addr:chararray,
    http_status:chararray,
    bytes_sent:long,
    request_method:chararray,
    uri:chararray,
    proxy_host:chararray,
    content_type:chararray,
    referer:chararray,
    x_forwarded_for:chararray,
    user_agent:chararray,
    accept_language:chararray,
    x_cs:chararray
);

counts = FOREACH log_fields GENERATE
    FLATTEN(org.wikimedia.analytics.kraken.pig.ComparePageviewDefinitions(uri,
                                                                          referer,
                                                                          user_agent,
                                                                          http_status,
                                                                          remote_addr,
                                                                          content_type,
                                                                          request_method))
    AS (strict:int, webstatscollector:int, wikistats:int);

grouped_counts = GROUP counts ALL;

grouped_counts = FOREACH grouped_counts GENERATE
    SUM(counts.strict) as sum_strict:int,
    SUM(counts.webstatscollector) as sum_webstatscollector:int,
    SUM(counts.wikistats) as sum_wikistats:int;

