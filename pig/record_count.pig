-- Outputs the number of records in $input

IMPORT 'include/load_webrequest.pig';
LOG_FIELDS = LOAD_WEBREQUEST('$input');
log_grouped = GROUP LOG_FIELDS ALL;
log_count = FOREACH log_grouped GENERATE COUNT(LOG_FIELDS);
DUMP log_count;
