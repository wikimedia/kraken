SELECT term, COUNT(t.session_id) as num
FROM (
    SELECT
        split(lower(regexp_replace(parse_url(ms.entry_referer, 'QUERY', 'q'), '\\+|%20', ' ')), '\\s+') AS kw,
        ms.session_id
    FROM mobile_sessions ms
    WHERE ms.dt >= '2013-03-00' AND ms.dt <= '2013-04'
) t
LATERAL VIEW explode(t.kw) terms AS term
WHERE NOT (term REGEXP '^(de|of|in|is|la|and|el|es|to|en|di|los|le|da|se|las|les|il|du|a|i|o|y|e)$')
GROUP BY term
ORDER BY num DESC
