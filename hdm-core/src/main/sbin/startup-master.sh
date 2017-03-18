#!/bin/bash
# DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8100"
business_path=./
cd $business_path
lib=`find lib -name *.jar | xargs`
port="8999"
mode="single-cluster"
if [ $# -gt 0 ]; then
 port="$1"
fi
if [ $# -gt 1 ]; then
 mode="$2"
fi

# java $DEBUG_OPTS -Dfile.encoding=UTF-8 -cp "$lib" -jar ./hdm-core-0.0.1.jar -m true -p $port -M $mode -f "./hdm-core.conf"
java $DEBUG_OPTS -Dfile.encoding=UTF-8 -cp "$lib" -jar ./hdm-core-0.0.1.jar -f "./hdm-core.conf" -m true "$@"