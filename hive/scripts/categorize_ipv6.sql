-- Compare: http://www.google.com/ipv6/statistics.html
SELECT count(*) as total, sum(t.is_6to4) as total_6to4, sum(t.is_teredo) as total_teredo
FROM (
    SELECT
        if(substr(m.ip_address, 0, 5) == '2002:', 1, 0) AS is_6to4,
        if(substr(m.ip_address, 0, 7) == '2001:0:', 1, 0) AS is_teredo
    FROM mobile_ipv6_march m 
) t
