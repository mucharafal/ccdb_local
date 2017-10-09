#!/bin/bash

cd `dirname $0`

# prepare Tomcat and compile the project
./compile.sh || exit 1

T="apache-tomcat"

TB="$T/bin"
TL="$T/lib"

# the minimal set of JARs to start Tomcat and load the servler
for jar in $TB/tomcat-juli.jar $TL/{catalina.jar,servlet-api.jar,tomcat-api.jar,tomcat-util.jar,tomcat-util-scan.jar,tomcat-coyote.jar,tomcat-jni.jar,annotations-api.jar,jaspic-api.jar,jasper.jar}; do
    jar -xf $jar
done

# remove useless (for this project) classes
rm -rf META-INF LICENSE NOTICE about.html ecj.1

rm -rf org/apache/catalina/{manager,filters,ssi,users}

ls org/apache/catalina/servlets/*.class | grep -v DefaultServlet | xargs rm
ls org/apache/catalina/valves/*.class | grep -v -E -e "ValveBase|ErrorReportValve" | xargs rm
ls org/apache/catalina/session/*.class | grep -v -E -e "StandardManager|ManagerBase|TooManyActiveSessionsException" | xargs rm

rm -rf org/apache/coyote/{ajp,http2}
rm -rf org/apache/jasper/{el,xmlparser,tagplugins,security,runtime}

ls org/apache/jasper/compiler/*.class | grep -v -E -e "JspRuntimeContext|TldCache|JspConfig|TagPluginManager|Node" | xargs rm

rm -rf org/apache/naming

for a in javax/servlet/resources/{*.dtd,*.xsd}; do
    echo -n > $a
done

ln -s ../build/classes/ch .

# package everything in a single JAR
jar -cfe local.jar \
    ch.alice.o2.ccdb.LocalEmbeddedTomcat ch/alice/o2/ccdb/LocalEmbeddedTomcat.class \
    ch javax org

# further compression and remove debugging information
pack200 --repack -G -O local.jar

# remove all intermediate folders
rm -rf javax org ch