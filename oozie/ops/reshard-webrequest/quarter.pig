-- Split webrequest data into time buckets, writing it back in the same format.
-- 
-- Apply vigorously when imports fall behind and generate a large number of mostly identical files.
-- Note you'll need to move the broken imports to a tmp dir first, and you'll need to be careful
-- of duplicate lines if several import runs fell behind and launched at the same time.

REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'

-- Script Parameters
-- 
-- Required:
--      input               -- Input data paths. Note that you'll want to avoid picking up data prior to 'date_bucket_hour',
                            -- as it will still fall into 'output_rest'.
--      output_bucket_dir   -- Output data path; value should omit the datetime component as each split will
                            -- append 'date_bucket_hour' and minute/second tokens to form the bucket's output dir.
                            -- Ex: Given:
                            --      date_bucket_hour=2013-04-16_03
                            --      output_bucket_dir=hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile
                            -- We will store the 15-29 minute-bucket at:
                            --      hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/2013-04-16_03.15.00
--      date_bucket_hour    -- Date used to split data into buckets; format must match 'yyyy-MM-dd_HH'.
--      output_rest         -- Output path for non-bucketed data.

DEFINE ToDateBucket org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd_HH:mm');

-- FIXME: LOAD_WEBREQUEST declares kafka_byte_offset as a double, which usually truncates it to scientific
-- notation in the output. This probably only matters in scripts where we want an exact copy, so whatever.
-- IMPORT 'include/load_webrequest.pig';
-- log_fields = LOAD_WEBREQUEST('$input');

log_fields = LOAD '$input' AS (
    kafka_byte_offset:chararray, -- <- boo double!
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


SPLIT log_fields INTO
    bucket0 IF ToDateBucket(timestamp) MATCHES '$date_bucket_hour:(0\\d|1[0-4])',
    bucket1 IF ToDateBucket(timestamp) MATCHES '$date_bucket_hour:(1[5-9]|2\\d)',
    bucket2 IF ToDateBucket(timestamp) MATCHES '$date_bucket_hour:(3\\d|4[0-4])',
    bucket3 IF ToDateBucket(timestamp) MATCHES '$date_bucket_hour:(4[5-9]|5\\d)',
    rest    OTHERWISE;

STORE bucket0 INTO '$output_bucket_dir/$date_bucket_hour.00.00' USING PigStorage();
STORE bucket1 INTO '$output_bucket_dir/$date_bucket_hour.15.00' USING PigStorage();
STORE bucket2 INTO '$output_bucket_dir/$date_bucket_hour.30.00' USING PigStorage();
STORE bucket3 INTO '$output_bucket_dir/$date_bucket_hour.45.00' USING PigStorage();

STORE rest    INTO '$output_rest'                               USING PigStorage();

