SELECT top_ips.ip_address, sum(top_ips.pageviews) AS hits
FROM (
    SELECT ts.ip_address, ts.pageviews
    FROM top_sessions ts
    WHERE ts.pageviews > 1000
) top_ips
GROUP BY top_ips.ip_address
ORDER BY hits DESC


SELECT ip_address, sum(pageviews) as hits
FROM top_sessions
WHERE pageviews > 1000
GROUP BY ip_address
ORDER BY hits DESC
