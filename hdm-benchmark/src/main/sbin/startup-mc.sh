#!/bin/bash
# DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8100"
business_path=./
cd $business_path
lib=`find lib -name *.jar | xargs`

masterAddress1="$1"
masterAddress2="$2"
dataPath="$3"
testTag="$4"
parallelism="$5"
masterPath1="akka.tcp://masterSys@${masterAddress1}/user/smsMaster"
masterPath2="akka.tcp://masterSys@${masterAddress2}/user/smsMaster"
if [ $# -gt 5 ]; then
param="$6"
java $DEBUG_OPTS -Dfile.encoding=UTF-8 -cp "$lib" org.hdm.core.benchmark.MultiClusterBenchmarkMain  $masterPath1 $masterPath2 $dataPath $testTag $parallelism $param
else
java $DEBUG_OPTS -Dfile.encoding=UTF-8 -cp "$lib" org.hdm.core.benchmark.MultiClusterBenchmarkMain $masterPath1 $masterPath2 $dataPath $testTag $parallelism
fi