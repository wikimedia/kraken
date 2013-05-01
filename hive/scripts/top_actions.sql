SELECT t.action, count(*) as num
FROM webrequest_article_views_actions t
WHERE t.action IS NOT NULL
GROUP BY t.action
ORDER BY num DESC
