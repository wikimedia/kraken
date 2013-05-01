SELECT
    reqs.request_timestamp,
    reqs.kafka_byte_offset,
    if(
        parse_url(reqs.uri, 'PATH') == '/w/index.php',
        parse_url(reqs.uri, 'QUERY', 'title'),
        substr(parse_url(reqs.uri, 'PATH'), 7)
    ) AS title,
    lower(parse_url(reqs.uri, 'QUERY', 'action')) AS action,
    lower(parse_url(reqs.uri, 'HOST')) AS domain,
    reqs.uri,
    split(reqs.ip_geo, '\\|')[0] as ip_address,
    split(reqs.ip_geo, '\\|')[1] as geo,
    split(reqs.http_status, '/')[1] AS http_status,
    split(reqs.http_status, '/')[0] AS cache_status,
    reqs.request_time AS response_time,
    reqs.referer,
    reqs.x_analytics,
    reqs.user_agent,
    substr(reqs.request_timestamp, 0, 10) AS dt
FROM webrequest_all_sampled_1000 reqs
WHERE
    reqs.uri REGEXP '^https?://[^/]+/(w|wiki)/(?!api\\.php).*'
    AND reqs.dt >= '2013-04' AND reqs.dt < '2013-05'
