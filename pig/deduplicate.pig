-- Deduplicates webrequest logs by grouping on
-- hostname,sequence,timestamp and bytes_sent.
-- We cannot use 'DISTINCT' since the Kafka Byte Offset may
-- be different if the duplicates existed in Kafka.
SET default_parallism 2;

IMPORT 'include/load_webrequest.pig';

LOG_FIELDS = LOAD_WEBREQUEST('$input');

GROUPED = GROUP LOG_FIELDS BY (hostname,sequence,timestamp,bytes_sent,uri);

FILTERED =  FOREACH GROUPED {
       LINE = LIMIT LOG_FIELDS 1;
       GENERATE FLATTEN(LINE);
};

STORE FILTERED INTO '$output';
