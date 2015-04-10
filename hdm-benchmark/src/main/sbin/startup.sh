#!/bin/bash
# DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8100"
business_path=./
cd $business_path
lib=`find lib -name *.jar | xargs`

masterAddress="$1"
dataPath="$2"
testTag="$3"
parallelism="$4"
masterPath="akka.tcp://masterSys@${masterAddress}/user/smsMaster"
if [ $# -gt 4 ]; then
param="$5"
java $DEBUG_OPTS -Dfile.encoding=UTF-8 -cp "$lib" -jar ./HDM-benchmark-0.0.1.jar $masterPath $dataPath $testTag $parallelism $param
else
java $DEBUG_OPTS -Dfile.encoding=UTF-8 -cp "$lib" -jar ./HDM-benchmark-0.0.1.jar $masterPath $dataPath $testTag $parallelism
fi



