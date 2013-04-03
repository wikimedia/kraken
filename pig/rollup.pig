REGISTER 'kraken-generic-0.0.2-SNAPSHOT-jar-with-dependencies.jar'
REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'

-- Script Parameters
--      Pass via `-p param_name=param_value`. Ex: pig myscript.pig -p date_bucket_regex=2013-03-24_00
-- Required:
--      input                                   -- Input data paths.
--      output                                  -- Output data path.
--      input_fields                            -- Comma-separated list of `name:type` fields used to parse the input. Ex: timestamp:chararray, country:chararray, device_class:chararray, device_os:chararray, num:int
--      rollup_group_fields                     -- Comma-separated list of field names to group on for rollup; `date_bucket` is automatically included. Ex: country, device_class, device_os
--      rollup_sum_field                        -- Field to sum for the rollup. Ex: num
--      date_input_field                        -- Date-field to use for time-bucketing, which becomes `date_bucket`. Ex: timestamp
--      date_input_format                       -- Date format string used to parse input timestamps from `date_input_field`. Ex: yyyy-MM-dd_HH
-- Optional:
%default date_bucket_format 'yyyy-MM';          -- Format applied to timestamps for aggregation into buckets. Default: monthly.
%default date_bucket_regex  '.*';               -- Regex used to filter the formatted date_buckets; must match whole line. Default: no filtering.

DEFINE DATE_BUCKET org.wikimedia.analytics.kraken.pig.ConvertDateFormat('$date_input_format', '$date_bucket_format');

data = LOAD '$input' AS ($input_fields);
data = FOREACH data GENERATE DATE_BUCKET($date_input_field) AS date_bucket:chararray, *;
data = FILTER data BY (date_bucket MATCHES '$date_bucket_regex');

data_rollup = FOREACH (GROUP data BY (date_bucket, $rollup_group_fields))
    GENERATE FLATTEN($0), SUM($1.$rollup_sum_field);
data_rollup = ORDER data_rollup BY date_bucket, $rollup_group_fields;

STORE data_rollup INTO '$output' USING PigStorage();
