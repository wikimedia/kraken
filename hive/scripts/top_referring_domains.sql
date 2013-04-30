SELECT t.referer_host, COUNT(t.session_id) as num
FROM (
    SELECT parse_url(ms.entry_referer, 'HOST') as referer_host, ms.session_id
    FROM mobile_sessions ms
    WHERE ms.dt >= '2013-03-00' AND ms.dt <= '2013-04'
) t
GROUP BY t.referer_host
ORDER BY num DESC
