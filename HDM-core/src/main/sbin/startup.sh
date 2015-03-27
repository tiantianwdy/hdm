#!/bin/bash
# DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8100"
business_path=./
cd $business_path
lib=`find lib -name *.jar | xargs`

port="$2"
masterAddress="$3"
slots="$4"
memory="$5"
masterPath="akka.tcp://masterSys@${masterAddress}/user/smsMaster"
if [ "$1" == 'master' ]; then
	java $DEBUG_OPTS -Xms$memory -Xmx$memory -Dfile.encoding=UTF-8 -cp "$lib" -jar ./HDM-core-0.0.1.jar -m true -p $port
elif [ "$1" == 'slave' ]; then
	java $DEBUG_OPTS -Xms$memory  -Xmx$memory -Dfile.encoding=UTF-8 -cp "$lib" -jar ./HDM-core-0.0.1.jar -m false -p $port -P $masterPath -s $slots
fi