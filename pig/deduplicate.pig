-- Deduplicates webrequest logs by grouping on
-- hostname,sequence,timestamp and bytes_sent.
-- We cannot use 'DISTINCT' since the Kafka Byte Offset may
-- be different if the duplicates existed in Kafka.
-- REGISTER 'kraken-pig-0.0.2-SNAPSHOT.jar'


IMPORT 'include/load_webrequest.pig';
LOG_FIELDS = LOAD_WEBREQUEST('$input');

-- LOG_FIELDS = FILTER LOG_FIELDS BY (timestamp >= '$lower_time_bound' AND timestamp < '$upper_time_bound');

GROUPED = GROUP LOG_FIELDS BY (hostname,sequence,timestamp,bytes_sent,uri);

FILTERED =  FOREACH GROUPED {
       LINE = LIMIT LOG_FIELDS 1;
       GENERATE FLATTEN(LINE);
};

STORE FILTERED INTO '$output';
