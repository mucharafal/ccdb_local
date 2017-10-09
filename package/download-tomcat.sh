#!/bin/bash

cd `dirname $0`

# Tomcat version to embed in this project
VER="8.5.23"

T="apache-tomcat-$VER"

if [ ! -d "$T" ]; then
    echo "Downloading Tomcat $VER"

    wget -nv "http://mirror.switch.ch/mirror/apache/dist/tomcat/tomcat-8/v$VER/bin/apache-tomcat-$VER.tar.gz" -O apache-tomcat.tar.gz || exit 1

    tar -xf apache-tomcat.tar.gz || exit 2
    rm apache-tomcat.tar.gz

    rm -rf apache-tomcat-$VER/{conf,logs,temp,webapps,work,LICENSE,NOTICE,RELEASE-NOTES,RUNNING.txt} apache-tomcat-$VER/bin/{*.sh,*.bat,*.tar.gz,*.xml}
fi

rm -f apache-tomcat
ln -s "$T" apache-tomcat