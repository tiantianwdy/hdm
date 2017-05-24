#!/bin/bash
# DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8100"
JAVA_HOME=""
HDM_HOME="./"
cd $HDM_HOME
lib=`find lib -name *.jar | xargs`

port="$2"
masterAddress="$3"
slots="$4"
memory="$5"
bPort="9091"
if [ $# -gt 5 ]; then
 bPort="$6"
fi
masterPath="akka.tcp://masterSys@${masterAddress}/user/smsMaster"
if [ "$1" == 'master' ]; then
	${JAVA_HOME}java $DEBUG_OPTS -Xms$memory -Xmx$memory -Dfile.encoding=UTF-8 -cp "$lib" -jar ./hdm-core-0.0.1.jar -m true -p $port -n cluster
elif [ "$1" == 'slave' ]; then
	${JAVA_HOME}java $DEBUG_OPTS -Dfile.encoding=UTF-8 -cp "$lib" -jar ./hdm-core-0.0.1.jar -m false -p $port -P $masterPath -s $slots -b $bPort -mem $memory -n cluster
fi