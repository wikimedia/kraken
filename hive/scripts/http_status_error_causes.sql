-- Boo, the UNIONs generate unoptimized subjobs (walking the whole dataset), which is no good.
-- See http_status_error_causes_all.sql for a much faster version using a temporary table.

SELECT *
FROM (
    SELECT 404 as status, m.uri, count(*) as num
    FROM webrequest_mobile m
    WHERE m.dt >= '2013-03-24' AND m.dt < '2013-03-25'
        AND cast(split(m.http_status, '/')[1] AS int) == 404
    GROUP BY m.uri
    ORDER BY num DESC
    LIMIT 250
    
    UNION ALL
    
    SELECT 500 as status, m.uri, count(*) as num
    FROM webrequest_mobile m
    WHERE m.dt >= '2013-03-24' AND m.dt < '2013-03-25'
        AND cast(split(m.http_status, '/')[1] AS int) == 500
    GROUP BY m.uri
    ORDER BY num DESC
    LIMIT 250
    
    UNION ALL
    
    SELECT 503 as status, m.uri, count(*) as num
    FROM webrequest_mobile m
    WHERE m.dt >= '2013-03-24' AND m.dt < '2013-03-25'
        AND cast(split(m.http_status, '/')[1] AS int) == 503
    GROUP BY m.uri
    ORDER BY num DESC
    LIMIT 250
) st
ORDER BY status ASC, num DESC
