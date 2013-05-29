-- Populate status table...
CREATE TABLE IF NOT EXISTS
    tmp_mobile_http_status_uri_march(
        status int,
        uri string,
        num int)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

INSERT INTO TABLE tmp_mobile_http_status_uri_march
SELECT cast(split(http_status, '/')[1] AS int) AS status, uri, count(*) AS num
FROM webrequest_mobile m
WHERE dt >= '2013-03' AND dt < '2013-04'
  AND cast(split(http_status, '/')[1] AS int) >= 400
GROUP BY cast(split(http_status, '/')[1] AS int), uri
ORDER BY status ASC, num DESC;



-- Then query it!


-- Fast, but can be inaccurate
SELECT * FROM (
    SELECT *
    FROM tmp_mobile_http_status_uri_march st
    WHERE num >= 100
    ORDER BY num DESC
    LIMIT 750
) top_st
ORDER BY status ASC, num DESC;


-- Slower, but accurate
SELECT *
FROM (
    SELECT status, uri, num
    FROM tmp_mobile_http_status_uri_march st
    WHERE status == 404 AND num >= 100
    ORDER BY num DESC
    LIMIT 250
    
    UNION ALL
    
    SELECT status, uri, num
    FROM tmp_mobile_http_status_uri_march st
    WHERE status == 500 AND num >= 100
    ORDER BY num DESC
    LIMIT 250
    
    UNION ALL
    
    SELECT status, uri, num
    FROM tmp_mobile_http_status_uri_march st
    WHERE status == 503 AND num >= 100
    ORDER BY num DESC
    LIMIT 250
) uns
ORDER BY status ASC, num DESC;

