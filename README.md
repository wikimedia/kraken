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

## Get libdclass

### Debian-like systems

If your an a Debian system, you can probably just grab the most recent .deb files of libdclass0 and libdclassjni0 for your architecture from [garage-coding](http://garage-coding.com/releases/libdclass-dev/) and install them using dpkg and can proceed to the next step.

### Non-Debian-like systems

If your not on a Debian system, you can try getting the needed files by hand or compiling dclass by yourself.

#### Getting files by hand

For an amd64 system with the most recent version being 2.0.14,
that would be:

    mkdir dclass
    cd dclass

    mkdir libdclass0
    cd libdclass0
    wget http://garage-coding.com/releases/libdclass-dev/libdclass0_2.0.14_amd64.deb
    ar x libdclass0_2.0.14_amd64.deb
    tar -xzvf data.tar.gz
    sudo mv usr/lib/libdclass.so.0.0.0 /usr/lib/libdclass.so.0
    sudo chmod +x /usr/lib/libdclass.so.0
    cd ..

    mkdir libdclassjni0
    cd libdclassjni0
    wget http://garage-coding.com/releases/libdclass-dev/libdclassjni0_2.0.14_amd64.deb
    ar x libdclassjni0_2.0.14_amd64.deb
    tar -xzvf data.tar.gz
    sudo mv usr/lib/libdclassjni.so.0.0.0 /usr/lib/libdclassjni.so
    sudo chmod +x /usr/lib/libdclassjni.so
    cd ..

    cd ..

You can test whether or not it's working by doing

    ldd /usr/lib/libdclassjni.so /usr/lib/libdclass.so.0

to see if the dynamic linker caught the files. You should get an output like:

    /usr/lib/libdclassjni.so:
            linux-vdso.so.1 (0x00007f43caf4b000)
            libdclass.so.0 => /usr/lib64/libdclass.so.0 (0x00007f43cab05000)
            libc.so.6 => /lib64/libc.so.6 (0x00007f43ca75a000)
            librt.so.1 => /lib64/librt.so.1 (0x00007f43ca551000)
            /lib64/ld-linux-x86-64.so.2 (0x00007f43caf4c000)
            libpthread.so.0 => /lib64/libpthread.so.0 (0x00007f43ca334000)
    /usr/lib/libdclass.so.0:
            linux-vdso.so.1 (0x00007fffdb3ff000)
            librt.so.1 => /lib64/librt.so.1 (0x00007f6948ba3000)
            libc.so.6 => /lib64/libc.so.6 (0x00007f69487f9000)
            libpthread.so.0 => /lib64/libpthread.so.0 (0x00007f69485db000)
            /lib64/ld-linux-x86-64.so.2 (0x00007f6948fef000)

(If numbers changed, that's not a problem. Only „Not Found” lines are a problem)

We do no longer need the dclass folder. You can remove the dclass folder again.

#### Compiling by yourself

Instructions on how to compile dclass by yourself are at:

https://github.com/wikimedia/dClass/blob/package/README

## openddr.dtree file

Get a openddr.dtree file from an [Analytics Team member](http://www.mediawiki.org/wiki/Analytics), and store it as

    /usr/share/libdclass/dtrees/openddr.dtree

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

## Maven configuration

Before running maven in the kraken repository, we need to configure maven. Copy (or merge, if you already have a Maven settings.xml) maven/example.settings.xml from your maven clone to $HOME/.m2/settings.xml. Note that this will change where Maven tries to download jars.

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
