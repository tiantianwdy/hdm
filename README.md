HDM
==============

[![Build Status](https://travis-ci.org/tiantianwdy/hdm.svg?branch=hdm-mc-dev)](https://travis-ci.org/tiantianwdy/hdm)

HDM (Hierarchy Distributed Matrix) is a light-weight, optimized, functional framework for data processing and analytics on large scale data sets.


# Build HDM from source code

HDM is built using Apache Maven. To build HDM from source code, go to the directory of hdm-core and run:

```shell
mvn -DskipTests clean install
```

Then unzip the hdm-engine.zip from the `target` folder, after then you can see all the components required for HDM.


# Quick start a HDM cluser


## Start master

Users can start the master node of HDM by execute the shell cmd `./hdm-deamon.sh start master [parameters]?` under the root folder of hdm-core, for example:

```shell
cd ./hdm-engine
./hdm-deamon.sh start master -p 8998 -h 127.0.1.1
```
The cmd above starts a master listening on port 8998 with host address 127.0.1.1

## Start a slave

Users can start a slave node by executing the shell cmd `./hdm-deamon.sh start slave  -p [port of slave] -P [address of the master] -s [number of cores] -mem [amount of JVM memory] -b [port for data transferring]`

```shell
cd ./hdm-core
./hdm-deamon.sh start slave -p 10001 -P 172.18.0.1:8998 -s 4 -mem 1G -b 9001
```


## Submit dependency to the server

Before start to execute a job, users need to submit the dependency for a HDM application by executing the shell cmd: `./hdm-client.sh submit [master url] [application ID] [application version] [dependency file] [author]`

```shell
./hdm-client.sh submit "akka.tcp://masterSys@127.0.1.1:8998/user/smsMaster/ClusterExecutor"
  | "defaultApp"
  | "0.0.1"
  | dwu
  | "/home/ubuntu/dev/workspace/hdm/hdm-benchmark/target/hdm-benchmark-0.0.1.jar"
```

An application may need multiple dependency libs, users can add more dependencies to a HDM applications by execute:

```shell
./hdm-client.sh addDep "akka.tcp://masterSys@127.0.1.1:8998/user/smsMaster/ClusterResourceLeader"
 | "defaultApp"
 | "0.0.1"
 | dwu
 | "/home/ubuntu/lib/hdm-benchmark/lib/jna-4.0.0.jar"
```

Once all the dependencies have been submitted to the cluster, the HDM application can be executed freely on the cluster without re-submitting the libs until users make changes on the code.

## Start HDM console

Users can start the HDM console by deploy the `hdm-console-0.0.1.war` file of hdm-console to any web servers such as Apache Tomcat or Jetty.


# Programming in HDM


## Use Maven to manage dependencies

To be able to program using HDM APIs, users just need to add hdm-core into their programming environment:

Add dependency using maven:

``` XML
    <dependency>
      <groupId>org.hdm</groupId>
      <artifactId>hdm-core</artifactId>
      <version>${hdm.version}</version>
    </dependency>
```


HDM provides pure functional programming interfaces for users to write their data-oriented applications. In HDM, functions are the first citizens. Operations are just wrappers of primitive functions in HDM.

## Primitives

The basic functions of HDM are listed as below:

Function | parameters | Description
---------|------------|------------
NullFunc | N/A | A empty function, which does nothing to the input
Map | f: T -> R | applies transformation function f to each record in the input to generate the output
GroupBy | f: T -> K | applies function f to get the key K of each record, group the inputs based on the identical keys.
FindBy | f: T -> Bool | applies the match function to filter out a subset of the inputs
reduce | f: (T, T) -> T | apply the reduce function to aggregate the records by folding them pair by pair.
Sort |f: (T,T) -> Bool | sort the input by applying the comparison function.
Flatten | N/A | transform a nested collection of input into the non-nested flat collection.
CoGroup | f1: T1 -> K, f2: T2 -> K| group two input data sets by finding the identical group key based on the two parameter functions. For each input, the records with the same key are organized in a collection.
JoinBy | f1: T1 -> K, f2: T2 -> K | join two input data sets by finding the identical group key based on the two parameter functions. For each input, the records with the same key are flatten into tuples.

Apart from basic primitives, HDM also provides more derived functions based on the data type for example, for Key-Value based records, HDM provides derived functions such as `reduceByKey`, `findByKey`, `findValues` and `mapKeys`.

## Actions

HDM is designed to be interactive with general programs during runtime, it delays the computation until a data collecting action is triggered. HDM contains a few actions that can trigger the execution of HDM applications on either remote or local HDM cluster.
Each of the Actions has specific return type for different purposes:

Action | Return Type | Description
---------|------------|------------
compute | HDMRefs| References of the HDMs which represents the meta-data (location, data type, size) of computed output.
sample | Iterator| An iterator object which can retrieve the a subset of sampled records from the computed output.
count | Long | Returns the number of records in the computed output.
traverse | Iterator | An iterator object which can retrieve all the records one by one from the computed output.
trace | ExecutionTrace | Returns a collection of execution traces for the last execution of the application.

## Interation Pattern

For the consideration of performance, after the computation is triggered, the client side program would obtain the results in a asynchronous manner.

For example, an user can print out the results of `WordCount` program using the code below:

```scala
wordcount.traverse(context = "10.10.0.100:8999") onComplete {
  case Success(resp) => resp.foreach(println)
  case other => // do nothing
}
```

## Examples

To better illustrate how to program in HDM, here are some examples as below:

### WordCount

```scala
val wordcount = HDM("/path/data").map(_.split(","))
            .flatMap(w => (w, 1))
            .reduceByKey(_ + _)
```

### TopK

```scala
val k = 100
val topK = HDM(path).map{w => w.split(",")}
           .map{arr => (arr(0).toFloat, arr)}
           .top(k)
```

### LinearRegression
```scala
val input = HDM("hdfs://127.0.0.1:9001/user/data")
val training = input.map(line => line.split("\\s+"))
               .map { arr =>
                  val vec = Vector(arr.drop(0))
                  DataPoint(vec, arr(0))
               }
val weights = DenseVector.fill(10){0.1 * Random.nextDouble}

for (i <- 1 to iteration){
  val w = weights
  val grad = training.map{ p =>
    p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
  }.reduce(_ + _).collect().next()
  weights -= grad
}

```




