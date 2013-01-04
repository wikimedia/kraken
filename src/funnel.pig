--analyze_stock.pig
register 'acme.jar';
define analyze com.acme.financial.AnalyzeStock();
daily
= load 'NYSE_daily' as (exchange:chararray, symbol:chararray,
date:chararray, open:float, high:float, low:float,
close:float, volume:int, adj_close:float);
grpd
= group daily by symbol;
analyzed = foreach grpd {
sorted = order daily by date;
generate group, analyze(sorted);
};


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
