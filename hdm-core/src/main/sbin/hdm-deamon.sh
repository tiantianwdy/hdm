#!/bin/bash
script_name="$0"

usage () {
  cat <<EOM

Usage: $script_name [start|stop] [master|slave] [options]

[Options]

	-h	hostname of the node
	-p	port of the node
	-P	parent/master of the node
	-f	the config file to initiate the node
	-mem	set the max memory for the JVM of the node
	-s	the number of slots for compucation
	-b	the port for the BlockManger for this node, which is used for data transfer during computation
	-M	the mode of the cluster, can be "single-cluster" (default) or "multi-cluster"
	-n	the node mode of the cluster, can be "app" or "cluster" (default).
EOM
}

echoerr () {
  echo 1>&2 "$@"
}

start_master () {
  echo "starting master..."
  ./startup-master.sh $@
  # pid=`nohup ./startup-master.sh $@ > "hdm-master.log" 2>&1 < /dev/null &`
  # echo "starting master completed at $pid"
}


start_slave () {
  echo "starting slave..."
  ./startup.sh slave $@
#  pid=`nohup ./startup.sh slave $@ > "hdm-slave.log" 2>&1 < /dev/null &`
#  echo "starting slave completed at $pid"
}


stop_master () {
  ps -ef | grep -E 'hdm-core-0.0.1.jar.*-m true' | awk '{print $2}' | xargs kill $1
  echo "Stopping master completed..."
}

stop_slave () {
  ps -ef | grep -E 'hdm-core-0.0.1.jar.*-m false' | awk '{print $2}' | xargs kill $1
  echo "Stopping slave completed..."
}

#
# Main
#
main () {
op="$1"
node_type="$2"

shift 2
echo "arguments: [$@]"

case "$op" in 

  start)
	if [ "$node_type" == "master" ]; then
	  start_master $@       
	elif [ "$node_type" == 'slave' ]; then
	  start_slave $@ 
	fi;;

  stop)
	echo "Stopping $node_type";
	if [ "$node_type" == "master" ]; then
	  stop_master $@       
	elif [ "$node_type" == 'slave' ]; then
	  stop_slave $@ 
	fi;;

  *)  
	echoerr "[ERROR]: unkown command format: $0";
	usage;
	exit 1;;
esac

}


#
# Entry
#
if [ $# == 0 ]; then
  echoerr "[ERROR]: unkown command format: $0"
  usage
  exit 1
else
  main $@
fi


