set default_parallel 7;
register kraken.jar;

loglines  = LOAD  '/user/diederik/pagecounts-raw/2012/2012-09/pagecounts-201209*'  USING org.wikimedia.analytics.kraken.loaders.PigStorageWithInputPath(' ','-tagsource') AS (path:chararray, project_code:chararray, title:chararray, views:int, response_size:long);
page_id = LOAD '/user/diederik/feedback/feedback_page_traffic.tsv' USING  PigStorage('\t') AS (page_id:long, page_title:chararray, posts:int);

english = FILTER loglines by project_code eq 'en';

page_id = FILTER page_id BY NOT (page_title MATCHES 'NULL');

pageTitle = FOREACH english GENERATE
          'date' AS timestamp:chararray,
          title,
          REPLACE(title, '(talk:)||(special:ArticleFeedbackv5/)', '') AS page_title:chararray,
          views;

joined = JOIN pageTitle BY page_title, page_id BY page_title USING 'replicated';

drop = FOREACH joined GENERATE
        page_id AS page_id:long,
        title AS title:chararray,
        timestamp AS timestamp:chararray,
        posts AS posts:int,
        views AS views:int;

grouped = GROUP drop BY (page_id, title, timestamp, posts);

result = FOREACH grouped GENERATE
   FLATTEN(group) AS (page_id:long, title:chararray, timestamp:chararray, posts:chararray),
   SUM(drop.views) AS views;

ordered = ORDER result BY page_id, title, timestamp;                 

STORE ordered INTO '/user/diederik/feedback/results' USING PigStorage(',');
