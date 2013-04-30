SELECT t.q, COUNT(t.session_id) as num
FROM (
    SELECT
        lower(regexp_replace(parse_url(ms.entry_referer, 'QUERY', 'q'), '\\+|%20', ' ')) AS q,
        ms.session_id
    FROM mobile_sessions ms
    WHERE ms.dt >= '2013-03-00' AND ms.dt <= '2013-04'
) t
GROUP BY t.q
ORDER BY num DESC
