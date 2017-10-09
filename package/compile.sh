#!/bin/bash

cd `dirname $0`

# make sure Tomcat is available
./download-tomcat.sh || exit 1

# same directory structure as for Eclipse
mkdir -p ../build/classes/

# add all Tomcat JARs to the classpath
T=apache-tomcat

CLASSPATH=

for jar in $T/bin/*.jar $T/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

export CLASSPATH

# and compile the project
find ../src -name \*.java | xargs javac -d ../build/classes