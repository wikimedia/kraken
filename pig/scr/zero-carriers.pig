SET default_parallel 1;

data = LOAD '/wmf/public/webrequest/zero_carrier_country/2013/03/*/*/carrier/*' AS (
    day_hour:chararray,
    language:chararray,
    project:chararray,
    site_version:chararray,
    country:chararray,
    carrier:chararray,
    num:int
);
carriers = FOREACH (GROUP data BY carrier) GENERATE FLATTEN($0), COUNT($1) AS num:int;
STORE carriers INTO '/user/dsc/zero/carriers-by-date';
fs -mv /user/dsc/zero/carriers-by-date/part-r-00000 /user/dsc/zero/carriers-by-date.tsv
