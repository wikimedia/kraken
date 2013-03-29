# Note: This file is managed by Puppet.

# Enable JMX connections on port 9998
SERVER_JVMFLAGS="-Dcom.sun.management.jmxremote.port=9998 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
