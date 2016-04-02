package org.nicta.hdm.benchmark

import org.junit.{After, Test}
import org.nicta.wdy.hdm.examples.{UservisitsSQLBenchmark, IterationBenchmark, KVBasedPrimitiveBenchmark}
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.executor.HDMContext._
import com.baidu.bpit.akka.messages.{AddMsg, Query}
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.{AbstractHDM, HDM}
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Success, Failure}
/**
 * Created by tiantian on 17/02/15.
 */
class TechfestDemo {

  val text =
    """
        this is a word count text
        this is line 2
        this is line 3
    """.split("\\s+")

  val text2 =
    """
        this is a word count text
        this is line 4
        this is line 5
        this is line 6
        this is line 7
    """.split("\\s+")


  @Test
  def testStartSlave(): Unit = {
    HDMContext.init(leader = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster")
    Thread.sleep(50000000)
  }


  @Test
  def testHDFSExecution(): Unit = {
    HDMContext.init(leader = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster")
    Thread.sleep(1000)
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings")
//    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits")
    val hdm = HDM(path, false)

    val wordCount = hdm.map{ w =>
      val as = w.split(",");
      (as(0).substring(0,3), as(1).toFloat)
    }
//      .groupBy(_._1)
        .reduceByKey(_ + _)
//      .findByKey(_.startsWith("s"))
      //.map(t => (t._1, t._2.map(_._2).reduce(_+_)))
//      .groupReduce(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))


    wordCount.sample(20, 500000)(parallelism = 2).foreach(println(_))

    Thread.sleep(50000000)
  }

  @Test
  def testIterations(): Unit ={
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
    //    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits"
    val parallelism = 2
    HDMContext.NETTY_BLOCK_SERVER_PORT = 9092
    HDMContext.init(leader = context)
    Thread.sleep(1500)

    val benchmark = new IterationBenchmark
    benchmark.testGeneralIteration(data, parallelism)
  }


  @Test
  def testPrimitiveBenchMark(): Unit ={
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
//    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits"
    val parallelism = 1
    val len = 3
//    val benchmark = new KVBasedPrimitiveBenchmark(context)
    val benchmark = new KVBasedPrimitiveBenchmark(context = context, kIndex = 0, vIndex = 1)
    HDMContext.NETTY_BLOCK_SERVER_PORT = 9092
    HDMContext.init(leader = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster")
    Thread.sleep(1500)
    val hdm =
    benchmark.testGroupBy(data,len, parallelism)
//    benchmark.testMultipleMap(data,len, parallelism)
//    benchmark.testMultiMapFilter(data,len, parallelism, "a")
//    benchmark.testFindByKey(data,len, parallelism, "a")
//    benchmark.testReduceByKey(data,len, parallelism)
//    benchmark.testMap(data,len, parallelism)

    onEvent(hdm, "compute")(parallelism)
    Thread.sleep(50000000)
  }

  def onEvent(hdm:AbstractHDM[_], action:String)(implicit parallelism:Int) = action match {
    case "compute" =>
      val start = System.currentTimeMillis()
      hdm.compute(parallelism).map { hdm =>
        println(s"Job completed in ${System.currentTimeMillis() - start} ms. And received response: ${hdm.id}")
        hdm.blocks.foreach(println(_))
        System.exit(0)
      }
    case "sample" =>
      //      val start = System.currentTimeMillis()
      hdm.sample(25, 500000)foreach(println(_))
    case "collect" =>
      val start = System.currentTimeMillis()
      val itr = hdm.collect()
      println(s"Job completed in ${System.currentTimeMillis() - start} ms. And received results: ${itr.size}")
    case x =>
  }


  @Test
  def testCache(): Unit ={
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
    //    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits"
    val parallelism = 1
    HDMContext.NETTY_BLOCK_SERVER_PORT = 9092
    HDMContext.init(leader = context)
    Thread.sleep(1500)

    val benchmark = new IterationBenchmark
    benchmark.testIterationWithCache(data, parallelism)
  }


  @Test
  def testCacheExplain(): Unit ={
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
    //    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits"
    implicit val parallelism = 1
    HDMContext.NETTY_BLOCK_SERVER_PORT = 9092
    HDMContext.init(leader = context)
    Thread.sleep(1500)
    var start = System.currentTimeMillis()
    var aggregation = 0F
    val path = Path(data)
//    val hdm = HDM(path)
//    HDMContext.explain(HDM(path), parallelism).foreach(println(_))
    val hdm = HDM(path).cached
//    hdm.children.foreach(println(_))
//    println(hdm)
    for(i <- 1 to 2) {
      val vOffset = 1 // only avaliable in this scope, todo: support external variables
      val agg = aggregation
      val computed = hdm.map{ w =>
        val as = w.split(",")
        as(vOffset).toFloat + i*agg
      }
//      HDMContext.explain(computed, parallelism).foreach(println(_))
      val res = computed.collect()(parallelism)
      println(res.size)
//      res.foreach(println(_))
//      aggregation += res.sum
      val end = System.currentTimeMillis()
      println(s"Time consumed for iteration $i : ${end - start} ms.")
      start = System.currentTimeMillis()
      Thread.sleep(100)
    }
    Thread.sleep(300)
  }


  @Test
  def testRegression():Unit = {
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
    //    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits"
//    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/1node/weather"
    val parallelism = 1
    HDMContext.NETTY_BLOCK_SERVER_PORT = 9092
    HDMContext.init(leader = context)
    Thread.sleep(1500)

    val benchmark = new IterationBenchmark(1, 1)
    benchmark.testLinearRegression(data, 1, parallelism)
  }

  @Test
  def testWeatherLRegression():Unit = {
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
    //    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
    //    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits"
    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/1node/weather"
    val parallelism = 1
    HDMContext.NETTY_BLOCK_SERVER_PORT = 9092
    HDMContext.init(leader = context)
    Thread.sleep(1500)

    val benchmark = new IterationBenchmark(1, 1)
    benchmark.testWeatherLR(data, 12, 3, parallelism, false)
  }

  @Test
  def testTeraSort():Unit = {
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
    //    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits"
    implicit val parallelism = 1
    HDMContext.NETTY_BLOCK_SERVER_PORT = 9092
    HDMContext.init(leader = context)
    Thread.sleep(1500)

    val benchmark = new KVBasedPrimitiveBenchmark(context)
    val hdm = benchmark.testTeraSort(dataPath = data)
    onEvent(hdm, "sample")
    Thread.sleep(15000000)
  }


  @Test
  def testSQLBenchmark():Unit = {
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
    //    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
        val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits"
//    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/1node/weather"
    implicit val parallelism = 1
    HDMContext.NETTY_BLOCK_SERVER_PORT = 9092
    HDMContext.init(leader = context)
    Thread.sleep(1500)

    val benchmark = new UservisitsSQLBenchmark
//    val hdm = benchmark.testSelect(data, parallelism, 3)
//    val hdm = benchmark.testWhere(data, parallelism, 3, 0.5F)
//    val hdm = benchmark.testOrderBy(data, parallelism, 3)
    val hdm = benchmark.testAggregation(data, parallelism, 3)

    onEvent(hdm, "collect")
    Thread.sleep(1500000)
  }

  @Test
  def testCogroup(): Unit ={
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/micro/rankings")
    implicit val parallelism = 1
    HDMContext.NETTY_BLOCK_SERVER_PORT = 9092
    HDMContext.init(leader = context)
    Thread.sleep(1500)

    val hdm = HDM(path)
    val data1 = hdm.map{ w =>
      val as = w.split(",")
      (as(1).toInt, as(2))
    }

    val data2 = hdm.map{ w =>
      val as = w.split(",")
      (as(2).toInt, as(1))
    }

    val res = data1.cogroup(data2, (d1:(Int, String))  => d1._1 % 100, (d2:(Int, String)) => d2._1 % 100)
    onEvent(res, "sample")
    Thread.sleep(15000000)
  }

  @After
  def after() {
    HDMContext.shutdown()
  }

}
