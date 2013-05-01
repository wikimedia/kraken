SELECT lower(content_type) as content_type, count(*) as num
FROM webrequest_mobile m
WHERE m.dt >= '2013-03' AND m.dt < '2013-04'
GROUP BY lower(content_type)
