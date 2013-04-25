-- Visitor ID Macros

-- Required jars:
--      REGISTER 'hdfs:///libs/datafu-0.0.9.jar';
--      REGISTER 'hdfs:///libs/piggybank.jar';
DEFINE __MD5            datafu.pig.hash.MD5();
DEFINE __ISOToUnix      org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();



-- Calculates a visitor_id from the hash of IP + UA from a request. The ID is stable across time.
-- Params:
--      ip_address      (chararray)     An IP address in dotted-quad notation. Ex: 208.168.10.3
--      user_agent      (chararray)     User-Agent string from the request.
-- Returns:
--      visitor_id      (chararray)     Calculated ID, expressed in hex.
DEFINE VisitorId(ip_address, user_agent) RETURNS $visitor_id {
    visit = (chararray) CONCAT((chararray)ip_address, (chararray)user_agent);
    $visitor_id = __MD5(visit);
}

DEFINE VisitorIdByUnixTime(ip_address, user_agent, unix_time) RETURNS $visitor_id {
    window = (chararray) ((long) unix_time >> 29);
    visit = (chararray) CONCAT(window, (chararray) CONCAT((chararray)ip_address, (chararray)user_agent));
    $visitor_id = __MD5(visit);
}

DEFINE VisitorIdByISOTime(ip_address, user_agent, iso_time) RETURNS $visitor_id {
    window = (chararray) ((long) __ISOToUnix(iso_time) >> 29);
    visit = (chararray) CONCAT(window, (chararray) CONCAT((chararray)ip_address, (chararray)user_agent));
    $visitor_id = __MD5(visit);
}

