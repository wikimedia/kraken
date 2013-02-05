REGISTER 'kraken-pig-0.0.1-SNAPSHOT.jar'
SET default_parallel 1;

DEFINE TO_DAY  org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd', 'yyyy-MM');

LOG_FIELDS     = LOAD '$input' USING PigStorage('\t') AS (
	timestamp:chararray,
	country:chararray,
	vendor:chararray,
	device:chararray,
	views:long
);
LOG_FIELDS     = FOREACH LOG_FIELDS GENERATE TO_DAY(timestamp) AS day, country, vendor, device, views;
LOG_FIELDS     = FOREACH (GROUP LOG_FIELDS BY (day, country, vendor, device) PARALLEL 1) GENERATE FLATTEN(group), SUM(LOG_FIELDS.views) as num;
--DUMP LOG_FIELDS;
STORE LOG_FIELDS into '$output';
