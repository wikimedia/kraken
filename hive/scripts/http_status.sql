SELECT cast(split(http_status, '/')[1] AS int) AS status, count(*) AS num
FROM webrequest_mobile m
WHERE dt >= '2013-03' AND dt < '2013-04'
GROUP BY cast(split(m.http_status, '/')[1] AS int)
ORDER BY status ASC, num DESC
