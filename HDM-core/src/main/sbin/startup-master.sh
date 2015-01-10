#!/bin/bash
DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8100"
business_path=./
cd $business_path
lib=`find lib -name *.jar | xargs`
java $DEBUG_OPTS -Dfile.encoding=UTF-8 -cp "$lib" -jar ./HDM-core-0.0.1.jar -m true -p 8999
