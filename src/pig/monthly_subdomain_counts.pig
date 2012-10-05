DEFINE EXTRACT org.apache.pig.builtin.REGEX_EXTRACT_ALL();
LOG_FIELDS = LOAD '$input' USING PigStorage(' ') AS (hostname:chararray, udplog_sequence:chararray, timestamp:chararray, request_time:chararray, remote_addr:chararray, http_status:chararray, bytes_sent:chararray, request_method:chararray, uri:chararray, proxy_host:chararray, content_type:chararray, referer:chararray, x_forwarded_for:chararray, user_agent);
-- only count text/html
LOG_FIELDS = FILTER LOG_FIELDS BY content_type == 'text/html';
-- only count 200 and 302 response statuses
LOG_FIELDS = FILTER LOG_FIELDS BY (http_status MATCHES '^.*(200|302)$');

-- Extract the Month and subdomain out of the request log fields
MONTH_SUBDOMAIN = FOREACH LOG_FIELDS GENERATE FLATTEN(EXTRACT(timestamp, '^.*(\\d\\d\\d\\d-\\d\\d)-.*')) as month:chararray, FLATTEN (EXTRACT(uri, 'https?://(en|ja|es|de|ru|fr)\\..+.+')) as subdomain:chararray;
-- Discard any null subdomains
MONTH_SUBDOMAIN = FILTER MONTH_SUBDOMAIN BY subdomain IS NOT NULL;
-- Group by month and subdomain
MONTH_SUBDOMAIN_GROUP = GROUP MONTH_SUBDOMAIN BY (month, subdomain) PARALLEL 3;
-- Generate a COUNT
MONTH_SUBDOMAIN_COUNT = FOREACH MONTH_SUBDOMAIN_GROUP GENERATE FLATTEN(group), COUNT($1) as num;
-- Save the results
STORE MONTH_SUBDOMAIN_COUNT into '$output';

-- This will generate output like:
-- 2012-09	de	2
-- 2012-09	en	14
-- 2012-09	es	1
-- 2012-09	fr	1
-- 2012-09	ja	4
-- 2012-09	ru	3
-- 2012-10	en	14
-- 2012-10	ja	6
-- 2012-10	ru	3

-- The Total Count has to be grouped and created separately from subdomain count
-- Run this if you want to generate a count of total requests per month.
-- MONTH_TOTAL_COUNT = FOREACH (GROUP MONTH_SUBDOMAIN BY month) GENERATE FLATTEN(group), COUNT(MONTH_SUBDOMAIN);
-- STORE MONTH_TOTAL_COUNT into '/user/otto/logs0/month_total_count.0';

