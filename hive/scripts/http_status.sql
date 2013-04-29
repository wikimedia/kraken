SELECT st.status, count(st.ts) as num
FROM (
    SELECT split(m.http_status, '/')[1] AS status, m.request_timestamp AS ts
    FROM webrequest_mobile m
    WHERE m.dt >= '2013-03-00' AND m.dt < '2013-04'
) st
GROUP BY st.status
