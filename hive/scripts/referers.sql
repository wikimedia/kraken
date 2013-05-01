SELECT 
    reqs.request_timestamp,
    lower(parse_url(reqs.entry_referer, 'HOST')) as referer_host,
    lower(regexp_replace(parse_url(reqs.entry_referer, 'QUERY', 'q'), '\\+|%20', ' ')) AS referer_search
    lower(parse_url(reqs.uri, 'HOST')) as request_host,
    reqs.uri as request_url,
    split(reqs.ip_geo, '\\|')[0] as ip_address,
    split(reqs.ip_geo, '\\|')[1] as geo
FROM webrequest_all_sampled_1000 reqs
WHERE reqs.dt >= '2013-04' AND reqs.dt <= '2013-05'
