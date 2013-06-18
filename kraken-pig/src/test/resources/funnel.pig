REGISTER 'kraken.jar';
DEFINE funnel org.wikimedia.analytics.kraken.pig.Funnel();

log = LOAD 'example.log' AS (timestamp:chararray, ip:chararray, url:chararray);
grpd = GROUP log BY ip;
funneled = foreach grpd {
   sorted = ORDER log BY timestamp;
   GENERATE group, FLATTEN(funnel(sorted)) AS (funneled:boolean, drop:chararray);
};
filtered FILTER funneled BY funneled == true;
DUMP filtered;
