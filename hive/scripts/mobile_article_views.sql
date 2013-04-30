INSERT INTO TABLE mobile_article_views PARTITION (dt)
SELECT
    m.request_timestamp,
    m.kafka_byte_offset,
    if(
        parse_url(m.uri, 'PATH') == '/w/index.php',
        parse_url(m.uri, 'QUERY', 'title'),
        substr(parse_url(m.uri, 'PATH'), 7)
    ) AS title,
    lower(parse_url(m.uri, 'QUERY', 'action')) AS action,
    lower(parse_url(m.uri, 'HOST')) AS domain,
    m.uri,
    m.x_analytics,
    m.ip_address,
    split(m.http_status, '/')[1] AS http_status,
    split(m.http_status, '/')[0] AS cache_status,
    m.request_time AS response_time,
    m.user_agent,
    m.referer,
    substr(m.request_timestamp, 0, 10) AS dt
FROM webrequest_mobile m
WHERE
    m.uri REGEXP '^https?://[^/]+/(w|wiki)/(?!api\\.php).*'
    AND m.dt >= '2013-03' AND m.dt < '2013-04'
