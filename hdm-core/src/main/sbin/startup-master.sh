#!/bin/bash
# DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8100"
JAVA_HOME=""
HDM_HOME="./"
cd $HDM_HOME
lib=`find lib -name *.jar | xargs`
port="8999"
mode="single-cluster"
if [ $# -gt 0 ]; then
 port="$1"
fi
if [ $# -gt 1 ]; then
 mode="$2"
fi

${JAVA_HOME}java $DEBUG_OPTS -Dfile.encoding=UTF-8 -cp "$lib" -jar ./hdm-core-0.0.1.jar -m true -n cluster -f "./hdm-core.conf" "$@"
