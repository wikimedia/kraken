SELECT num AS num_ua_for_ip, count(ip_address) AS times_seen
FROM (
    SELECT ip_address, count(visitor_id) as num
    FROM top_sessions ts
    GROUP BY ip_address
) ip_ua
GROUP BY num
ORDER BY times_seen DESC

