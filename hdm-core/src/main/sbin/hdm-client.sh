#!/bin/bash
# JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8100"
JAVA_HOME=""
HDM_HOME=./
cd $HDM_HOME
lib=`find . -name *.jar | xargs`

${JAVA_HOME}java $JAVA_OPTS -Xms256m -Xmx256m -Dfile.encoding=UTF-8 -cp "$lib" org.hdm.core.server.HDMClient "$@"
