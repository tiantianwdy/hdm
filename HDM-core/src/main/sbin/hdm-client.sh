#!/bin/bash
# JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8100"
business_path=./
cd $business_path
lib=`find . -name *.jar | xargs`

java $JAVA_OPTS -Xms256m -Xmx256m -Dfile.encoding=UTF-8 -cp "$lib" org.nicta.wdy.hdm.server.HDMClient "$@"
