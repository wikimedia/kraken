set default_parallel 7;
register kraken.jar;

loglines = LOAD '/user/diederik/pagecounts-raw/2012/2012-09/pagecounts-20120930-0*' USING org.wikimedia.analytics.kraken.pig.PigStorageWithInputPath(' ','-tagsource') AS (path:chararray, project_code:chararray, title:chararray, views:int, response_size:long);
loglines = FILTER loglines by project_code MATCHES 'en';
loglines = FOREACH loglines GENERATE SUBSTRING(path, 11, 19) AS timestamp:chararray, title, views;
feedback = LOAD '/user/diederik/feedback/feedback_page_traffic.tsv' USING PigStorage('\t') AS (page_id:long, page_title:chararray, posts:int);
feedback = FILTER feedback BY NOT (page_title MATCHES 'NULL');
titles  = FOREACH feedback GENERATE page_title, CONCAT('Talk:',page_title) AS talk_title:chararray, CONCAT('Special:ArticleFeedbackv5/', page_title) AS feedback_title:chararray;
titles_feedback_joined = JOIN titles BY page_title, feedback BY page_title;
feedback = FOREACH feedback GENERATE page_id, timestamp, posts, FLATTEN(titles.$0.(page_title, talk_title, feedback_title) AS title:chararray;
--titles  = FOREACH titles GENERATE TOTUPLE(page_title, talk_title, feedback_title);
--feedback = FOREACH feedback GENERATE page_id, posts, FLATTEN(titles.$0.(page_title, talk_title, feedback_title)) AS title:chararray;
joined = JOIN loglines BY title, feedback BY title;
feedback = GROUP joined BY (timestamp, feedback::title, page_id);
feedback = FOREACH feedback GENERATE FLATTEN (group) AS (timestamp:chararray, title:chararray, page_id:long), SUM(joined.views), MIN(joined.posts);
feedback = ORDER feedback BY page_id, title, timestamp;
dump feedback;
store feedback into '/user/diederik/feedback/results/' using PigStorage(',');
