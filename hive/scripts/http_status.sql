SELECT st.status, count(st.ts) as num
FROM (
    SELECT split(m.http_status, '/')[1] AS status, m.request_timestamp AS ts
    FROM webrequest_mobile m
    WHERE NOT (m.http_status LIKE '%/2%')
) st
GROUP BY st.status
;
