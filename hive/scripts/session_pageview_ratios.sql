SELECT sum(special_pageviews) as total_special_views, sum(pageviews) as total_views
FROM mobile_sessions ms
WHERE dt >= '2013-03-00' AND dt <= '2013-04'
--
SELECT count(special_pageviews) AS sessions_w_special_view
FROM mobile_sessions ms
WHERE special_pageviews > 0 AND dt >= '2013-03-00' AND dt <= '2013-04'
--
