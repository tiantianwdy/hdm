#!/bin/bash
# DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8100"
JAVA_HOME=""
HDM_HOME="./"
HDM_VERSION="0.1-SNAPSHOT"
cd $HDM_HOME
lib=`find lib -name *.jar | xargs`

nodeType="$1"
shift

if [ $nodeType == 'master' ]; then
	${JAVA_HOME}java $DEBUG_OPTS -Dfile.encoding=UTF-8 -cp "$lib" -jar ./hdm-engine-${HDM_VERSION}.jar -m true -n cluster -f "./hdm-core.conf" "$@"
elif [ $nodeType == 'slave' ]; then
	${JAVA_HOME}java $DEBUG_OPTS -Dfile.encoding=UTF-8 -cp "$lib" -jar ./hdm-engine-${HDM_VERSION}.jar -m false -n cluster -f "./hdm-core.conf" "$@"
fi