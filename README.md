# Kraken

A robust and legendary analytics system of giant proportions said to have dwelt off the coasts of Norway and Iceland.

[![Build Status](https://travis-ci.org/wikimedia/kraken.png)](https://travis-ci.org/wikimedia/kraken)

Installing Kraken
=================

Operating System
----------------

Install Ubuntu 12.04  Precise.

You can get it from here http://releases.ubuntu.com/precise/

Prefered version would be 64bit.

You can even [put it in a VirtualBox](https://www.virtualbox.org/).

If you have a Mac , you will have to compile Dclass yourself (find details about this in the [Building for OSX section here](https://github.com/wikimedia/dClass/blob/package/README)).

Clone Kraken
--------------------

    git clone git@github.com:wikimedia/kraken.git


Install GeoIP
-------------

    sudo aptitude install libgeoip-dev libgeoip1

After this, get the latest `GeoIPCity.dat` file, which is provided by Maxmind.

Ask [one of the members of the Analytics Team for this file](http://www.mediawiki.org/wiki/Analytics) , they will provide you with one.

If noone is available and you have access to the `stat1` machine, you can find one GeoIPCity.dat in `/home/spetrea/maxmind_archive/GeoIP_City*.gz`.

Configure Maven
---------------

You have to configure your `~/.m2/settings.xml`.

You can start with the [example.settings.xml](https://github.com/wikimedia/kraken/blob/master/maven/example.settings.xml provided here).

Dclass
------

Get the Ubuntu package `libdclass-dev` [from here](http://garage-coding.com/releases/libdclass-dev/).

    wget http://garage-coding.com/releases/libdclass-dev/libdclass-dev_2.0.12_amd64.deb
    sudo dpkg -i libdclass-dev_2.0.12_amd64.deb


Install JDK 6
-------------

Add these two lines at the end of your `/etc/apt/sources.list`

    sudo add-apt-repository ppa:webupd8team/java
    sudo apt-get update
    sudo apt-get install oracle-java6-installer

Now run `java -version` and make sure it says that you're running java 6.

If for some reason you're still running some other version do 

    sudo update-java-alternatives -s java-6-oracle

And finally for environment variables

    sudo apt-get install oracle-java7-set-default

Install IDEA
------------

Get the IDE called IDEA from IntelliJ [here](http://www.jetbrains.com/idea/).

The Community edition suffices.

Start IDEA. It will ask you which version of JDK you want to use, you will tell it you want jdk6 (not openjdk6, but jdk6 from Oracle).

Now import the Kraken project from where you cloned it.

Now you can run individual tests easily.


Building Kraken
---------------

Information about [building kraken is available in the build.md here](https://github.com/wikimedia/kraken/blob/master/build.md).

