SELECT st.cache_host, st.cache_status, st.http_status, st.x_analytics, count(*) as num
FROM (
    SELECT
        m.cache_host,
        split(m.http_status, '/')[0] AS cache_status,
        split(m.http_status, '/')[1] AS http_status,
        -- str_to_map(x_analytics, ';', '=')['zero'] AS mcc_mnc,
        m.x_analytics
    FROM webrequest_mobile m
    WHERE m.dt >= '2013-03-24' AND m.dt < '2013-03-25'
) st
GROUP BY st.cache_host, st.cache_status, st.http_status, st.x_analytics
ORDER BY cache_host, cache_status, http_status, x_analytics

