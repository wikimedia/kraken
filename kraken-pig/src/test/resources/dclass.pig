--REGISTER target/kraken-dclass.jar

DEFINE DCLASS org.wikimedia.analytics.kraken.pig.UserAgentClassifier();

USERAGENT_STRINGS = LOAD 'src/test/java/resources/useragentstrings.txt' USING PigStorage('\t') AS (user_agent:CHARARRAY);

DEVICES = FOREACH USERAGENT_STRINGS GENERATE FLATTEN(DCLASS(user_agent)) AS device;

DUMP DEVICES;
