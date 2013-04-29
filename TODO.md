# todo

* Replace all our crazy and random timestamps with truncated ISO timestmps (using space-sep) using `org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToHour` etc.
* Common URL structure for jobs: `/wmf/data/CLIENT/DATA_DOMAIN/JOB/...`
* Common naming convention for jobs, & their workflow + coordinator files
* Figure out plan for jars in HDFS -- pig macro? UDF to calculate classpath from maven coords?
* Stopgap: Script to build and sync UDF jars
* Convention for dimensional breakouts, rollups -- use Hive?
* /libs/kraken/datasets.xml

* Try out Hive Action in Oozie for generating rollups
* Oozie Bundles only seem useful for connected/related coordinators with different frequencies 


