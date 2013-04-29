SELECT h.site_mode, bin.x, cast(bin.y as BIGINT) as y
FROM (
  SELECT ms.site_mode, histogram_numeric(ms.pageviews, 10) AS hist
  FROM mobile_sessions ms
  WHERE dt >= '2013-03-00' AND dt <= '2013-04'
  GROUP BY ms.site_mode
) h
LATERAL VIEW explode(h.hist) hhist AS bin
