# Kraken

A robust and legendary analytics system of giant proportions said to have dwelt off the coasts of Norway and Iceland.

[![Build Status](https://travis-ci.org/wikimedia/kraken.png)](https://travis-ci.org/wikimedia/kraken)

# Installing Kraken

## Install JDK 6

You'll need Java in at least version 6. For Ubuntu-like systems, you can use the instructions from the next item. For other distributions, please look up in your distribution's manual on how to install Java 6.

### Ubuntu-like systems

Add these two lines at the end of your `/etc/apt/sources.list`

    sudo add-apt-repository ppa:webupd8team/java
    sudo apt-get update
    sudo apt-get install oracle-java6-installer

Now run `java -version` and make sure it says that you're running java 6.

If for some reason you're still running some other version do 

    sudo update-java-alternatives -s java-6-oracle

And finally for environment variables

    sudo apt-get install oracle-java7-set-default

## Clone Kraken

Run the following command to create a kraken subdirectory in the current folder and clone kraken into

    git clone https://github.com/wikimedia/kraken.git

## GeoIP files

Get the

* GeoIPCity.dat,
* GeoIP.dat,
* GeoIPRegion.dat, and
* GeoIPv6.dat

files from an [Analytics Team member](http://www.mediawiki.org/wiki/Analytics) and store them in /usr/share/GeoIP.

If noone is available and you have access to the `stat1` machine, you can find one GeoIPCity.dat in `/home/spetrea/maxmind_archive/GeoIP_City*.gz`.

## Test data

Get the

* testdata_desktop.csv,
* testdata_mobile.csv, and
* testdata_session.csv

files from an [Analytics Team member](http://www.mediawiki.org/wiki/Analytics) and store them in kraken-pig/src/test/resources in your clone of kraken.

## Running tests

To test whether things worked out, run

    mvn test

## You're done :-)

## Programming for/in/with Kraken

If you want to start coding around Kraken, the Analytics team mostly uses IDEA from IntelliJ.

Get the IDE called IDEA from IntelliJ [here](http://www.jetbrains.com/idea/).

The Community edition suffices.

Start IDEA. It will ask you which version of JDK you want to use, you will tell it you want jdk6 (not openjdk6, but jdk6 from Oracle).

Now import the Kraken project from where you cloned it.

Now you can run individual tests easily.

Building Kraken
---------------

Further information about [building kraken is available in the build.md here](https://github.com/wikimedia/kraken/blob/master/build.md).
