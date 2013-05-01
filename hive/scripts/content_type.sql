SELECT content_type, count(*) as num
FROM webrequest_mobile m
WHERE m.dt >= '2013-03-24' AND m.dt < '2013-03-25'
GROUP BY content_type
