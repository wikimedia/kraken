-- Usage:
-- pig -p input=/hdfs/path/to/access_logs* -p output=/hdfs/path/to/output_directory -f project_language_counts.pig
--
-- This requires that akela-0.5-SNAPSHOT.jar (from https://github.com/mozilla-metrics/akela)
-- and maxmind-geoip-1.2.5.jar (from http://mvnrepository.com/artifact/org.kohsuke/geoip/1.2.5)
-- are in the Java CLASSPATH, and that /usr/share/GeoIP/GeoIPCity.dat exists.
--
-- NOTE: This is meant to operate on Wikimedia web access log files, not regular Apache format log files.


REGISTER 'akela-0.5-SNAPSHOT.jar' 
REGISTER 'maxmind-geoip-1.2.5.jar'

-- I get Heap Errors if I don't set this when working with large inputs.
set pig.exec.nocombiner true;

-- use the GeoIpLookup function from https://github.com/mozilla-metrics/akela/blob/master/src/main/java/com/mozilla/pig/eval/geoip/GeoIpLookup.java
DEFINE GeoIpLookup com.mozilla.pig.eval.geoip.GeoIpLookup('/usr/share/GeoIP/GeoIPCity.dat');
-- Load in the access log data
LOG_FIELDS           = LOAD '$input' USING PigStorage(' ') AS (hostname:chararray, udplog_sequence:chararray, timestamp:chararray, request_time:chararray, remote_addr:chararray, http_status:chararray, bytes_sent:chararray, request_method:chararray, uri:chararray, proxy_host:chararray, content_type:chararray, referer:chararray, x_forwarded_for:chararray, user_agent);
-- Geocode the remote_addr IP
GEO_DATA             = FOREACH LOG_FIELDS GENERATE FLATTEN (GeoIpLookup(remote_addr)) AS (country:chararray, country_code:chararray, region:chararray,  city:chararray,  postal_code:chararray, metro_code:int);
-- Extract the country code from the Geocoded results
COUNTRY_CODE         = FOREACH GEO_DATA GENERATE country_code;
-- Group by country code and count
COUNTRY_COUNT        = FOREACH (GROUP COUNTRY_CODE BY $0 PARALLEL 28) GENERATE $0, COUNT($1) as num;
-- Sort the results
COUNTRY_COUNT_SORTED = ORDER COUNTRY_COUNT BY num DESC;
-- Save the results into the $output directory.
STORE COUNTRY_COUNT_SORTED into '$output';