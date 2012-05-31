#!/bin/bash

# Builds Scribe and dependencies from simplegeo's forks
# of Facebook's thrift, fb303, and scribe.
# 

# set up an apt repo, just because it is nicer to be able to use apt than dpkg.
# become root to make this bit easier


echo -e "\n######################\nSetting up apt repository...\n######################\n"
sudo -s

apt-get install nginx
echo 'server {
    listen   80 default;
    server_name  default;

    location / {
        root   /var/www/apt;
        autoindex on;
        allow all;
    }
}' > /etc/nginx/sites-available/default
    
service nginx restart
    
mkdir -p /var/www/apt/conf
echo 'Origin: Andrew Otto
Label: Wikimedia Analytics
Codename: lucid
Architectures: source amd64
Components: main 
Description: Packages for the Wikimedia Analytics team.' > /var/www/apt/conf/distributions
    
echo '## Wikimedia Analytics apt repository
deb http://build1.pmtpa.wmflabs lucid main
deb-src http://build1.pmtpa.wmflabs lucid main' > /etc/apt/sources.list.d/wikimedia.analytics.list

# Cloudera Hadoop
# CDH is needed to compile scribe with hdfs support below.
echo 'deb http://archive.cloudera.com/debian lucid-cdh3 contrib
deb-src http://archive.cloudera.com/debian lucid-cdh3 contrib' > /etc/apt/sources.list.d/cloudera.list
apt-get update
apt-get install -y hadoop-0.20 hadoop-0.20-sbin hadoop-0.20-native


# create a build directory
exit # (don't need to be root to build packages)
mkdir -p $HOME/deb/{build,packages}
    
# Thrift (simplegeo)
# need a lot of stuff!
sudo apt-get install -y reprepro debhelper python-all-dev git-core libboost-dev libboost-test-dev libboost-all-dev libboost-program-options-dev libevent-dev automake libtool flex bison pkg-config g++ libssl-dev sun-java6-jdk sun-java6-jre mono-gmcs ant libmono-dev erlang-base ruby1.8-dev python-all-dbg ruby rake rubygems librspec-ruby mongrel libcommons-lang-java libmono-system-web2.0-cil php5 php5-dev libslf4j-java python-stdeb

echo -e "######################\n# Building Thrift...\n######################\n"
cd $HOME/deb/build
git clone https://github.com/simplegeo/thrift.git
cd thrift
./bootstrap.sh && ./configure
dpkg-buildpackage -rfakeroot -tc

# put packages in apt repository
cp -v $HOME/deb/build/*.{deb,dsc,changes,gz} $HOME/deb/packages/
cd /var/www/apt
sudo reprepro includedeb lucid $HOME/deb/packages/*.deb
for f in $HOME/deb/packages/*.dsc; do sudo reprepro includedsc lucid $f; done
sudo apt-get update
sudo apt-get -y --force-yes install libthrift0 libthrift-java libthrift-dev python-thrift thrift-compiler

# fb303 (simplegeo)  
echo -e "######################\n# Building fb303...\n######################\n"
cd $HOME/deb/build
git clone https://github.com/simplegeo/thrift-fb303
cd thrift-fb303
./bootstrap.sh --with-thriftpath=/usr && ./configure --with-thriftpath=/usr
dpkg-buildpackage -rfakeroot -tc

# put packages in apt repository
cp -v $HOME/deb/build/thrift-fb303*.{deb,dsc,changes,gz} $HOME/deb/packages/
cd /var/www/apt
sudo reprepro includedeb lucid $HOME/deb/packages/thrift-fb303_0.1-4_amd64.deb
sudo reprepro includedsc lucid $HOME/deb/packages/thrift-fb303_0.1-4.dsc
sudo apt-get update
sudo apt-get -y --force-yes install thrift-fb303

# build fb303 python package
cd $HOME/deb/build/thrift-fb303/py
python setup.py --command-packages=stdeb.command bdist_deb

# put python-fb303 in apt repository
cp -v $HOME/deb/build/thrift-fb303/py/deb_dist/*.{deb,dsc,changes,gz} $HOME/deb/packages/
cd /var/www/apt
sudo reprepro includedeb lucid $HOME/deb/packages/python-fb303_1.0-1_all.deb
sudo reprepro includedsc lucid $HOME/deb/packages/fb303_1.0-1.dsc
sudo apt-get update
sudo apt-get -y --force-yes install python-fb303


# Scribe (simplegeo)
echo -e "######################\n# Building Scribe...\n######################\n"
cd $HOME/deb/build
apt-get install -y --force-yes hadoop libhdfs0-dev libboost-filesystem-dev python-stdeb
git clone https://github.com/simplegeo/scribe.git scribe
cd scribe
export JAVA_HOME="/usr/lib/jvm/java-6-sun"
export CPPFLAGS="-I$JAVA_HOME/include -I$JAVA_HOME/include/linux"
export LDFLAGS="-L$JAVA_HOME/jre/lib/amd64/server"
./bootstrap.sh --prefix=/usr --with-thriftpath=/usr --with-fb303path=/usr --enable-hdfs --with-hadooppath=/usr
./configure    --prefix=/usr --with-thriftpath=/usr --with-fb303path=/usr --enable-hdfs --with-hadooppath=/usr
dpkg-buildpackage -rfakeroot -tc

# put packages in apt repository
cp -v $HOME/deb/build/scribe_2.2-sg11*.{deb,dsc,changes,gz} $HOME/deb/packages/
cd /var/www/apt
sudo reprepro includedeb lucid $HOME/deb/packages/scribe_2.2-sg11_amd64.deb
sudo reprepro includedsc lucid $HOME/deb/packages/scribe_2.2-sg11.dsc
sudo apt-get update
sudo apt-get install -y --force-yes scribe

# symlink libjvm.so - This has got to be a hack we can fix better than this, no?
sudo ln -s /usr/lib/jvm/java-6-sun/jre/lib/amd64/server/libjvm.so /usr/lib/libjvm.so

# scribe python .deb
cd $HOME/deb/build/scribe/lib/py
python setup.py --command-packages=stdeb.command bdist_deb
# put packages in apt repository
cp -v $HOME/deb/build/scribe/lib/py/deb_dist/*.{deb,dsc,changes,gz} $HOME/deb/packages/
cd /var/www/apt
sudo reprepro includedeb lucid $HOME/deb/packages/python-scribe_2.2-1_all.deb
sudo reprepro includedsc lucid $HOME/deb/packages/scribe_2.2-1.dsc
sudo apt-get update
sudo apt-get install -y --force-yes python-scribe

cd $HOME/deb

echo -e "\n\nDONE\n"
