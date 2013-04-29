SET job.name 'xanalytics-mfm'
SET mapreduce.job.queuename 'adhoc'

%default input_prefix 'hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile'
%default input_date '2013-04-21'
%default input '$input_prefix/$input_date*'
-- %default input 'hdfs:///wmf/raw/webrequest/webrequest-wikipedia-mobile/dt=2013-04-21*'
%default output 'hdfs:///user/dsc/xanalytics/$input_date'


REGISTER 'hdfs:///libs/kraken-0.0.2/kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'hdfs:///libs/kraken-0.0.2/kraken-pig-0.0.2-SNAPSHOT.jar'

DEFINE ToDateBucket org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', 'yyyy-MM-dd');


IMPORT 'hdfs:///libs/kraken/pig/include/load_webrequest.pig';
log_fields = LOAD_WEBREQUEST('$input');
has_xcs = FILTER log_fields BY ( (x_cs is not null) AND (x_cs != '') AND (x_cs != '-') );
-- has_xcs = FILTER has_xcs BY (x_cs MATCHES '.*?\\bmf-m=.*');
xa = FOREACH has_xcs GENERATE ToDateBucket(timestamp) as date_bucket:chararray, x_cs;
xa = FOREACH (GROUP xa BY (date_bucket, x_cs)) GENERATE FLATTEN($0), COUNT($1) as num:int;
xa = ORDER xa BY *;

STORE xa INTO '$output';

