REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'


-- Script Parameters
--      Pass via `-p param_name=param_value`. Ex: pig myscript.pig -p date_bucket_regex=2013-03-24_00
-- Required:
--      input                                   -- Input data paths.
--      output                                  -- Output path for carrier data.

%default date_bucket_format 'yyyy-MM-dd';       -- Format applied to timestamps for aggregation into buckets. Default: daily.
%default date_bucket_regex '.*';                -- Regex used to filter the formatted date_buckets; must match whole line. Default: no filtering.

DEFINE DATE_BUCKET       org.wikimedia.analytics.kraken.pig.ConvertDateFormat('yyyy-MM-dd\'T\'HH:mm:ss', '$date_bucket_format');
DEFINE PAGEVIEW_TYPE     org.wikimedia.analytics.kraken.pig.PageViewEvalFunc();
DEFINE IS_ZERO_PAGEVIEW  org.wikimedia.analytics.kraken.pig.ZeroFilterFunc('default');
DEFINE ZERO              org.wikimedia.analytics.kraken.pig.Zero();

IMPORT 'include/load_webrequest.pig'; -- See include/load_webrequest.pig
log_fields = LOAD_WEBREQUEST_GEOCODED('$input');

/*
    Only keep log lines for which all of the following is true:
     - X-CS/X-Analytics header is present
     - timestamp matches $date_bucket_format
     - host matches *.wikipedia.org
     - request is a zero pageview

    Note: We push the filter up as far as possible to minimize the data we compute on.
*/

log_fields = FILTER log_fields BY (
  DATE_BUCKET(timestamp) MATCHES '$date_bucket_regex' AND
  IS_ZERO_PAGEVIEW(uri, x_analytics)
);

zero_fields = FOREACH log_fields GENERATE
  DATE_BUCKET(timestamp)      AS date_bucket:chararray,
  FLATTEN(PAGEVIEW_TYPE(uri)) AS (language:chararray, project:chararray, site:chararray),
  FLATTEN(ZERO(x_analytics))         AS (carrier:chararray, carrier_iso:chararray),
  country;

count_group   = GROUP zero_fields BY (date_bucket, language, project, site, country, carrier);
carrier_count = FOREACH count_group GENERATE FLATTEN(group), COUNT(zero_fields) AS num:long;

-- save results into $output
STORE carrier_count INTO '$output' USING PigStorage();
--DUMP count;
