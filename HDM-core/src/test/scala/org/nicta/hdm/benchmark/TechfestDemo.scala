package org.nicta.hdm.benchmark

import org.junit.{After, Test}
import org.nicta.wdy.hdm.benchmark.KVBasedPrimitiveBenchmark
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.executor.HDMContext._
import com.baidu.bpit.akka.messages.{AddMsg, Query}
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.HDM
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


    wordCount.sample(20)(2) onComplete {
      case Success(hdm) =>
        println("Job completed and received response:" + hdm)
        hdm.foreach(println(_))
      case Failure(t) =>
        println("Job failed because of: " + t)
        t.printStackTrace()
    }

    Thread.sleep(50000000)
  }


  @Test
  def testBenchMark(): Unit ={
    val context = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster"
    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
//    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/micro/uservisits"
    val parallelism = 2
    val len = 3
//    val benchmark = new KVBasedPrimitiveBenchmark(context)
    val benchmark = new KVBasedPrimitiveBenchmark(context = context, kIndex = 0, vIndex = 1)
    HDMContext.NETTY_BLOCK_SERVER_PORT = 9092
    HDMContext.init(leader = "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster")
    Thread.sleep(1500)
//    benchmark.testGroupBy(data,len, parallelism)
//    benchmark.testMultipleMap(data,len, parallelism)
//    benchmark.testMultiMapFilter(data,len, parallelism, "a")
//    benchmark.testFindByKey(data,len, parallelism, "a")
    benchmark.testReduceByKey(data,len, parallelism)
//    benchmark.testMap(data,len, parallelism)
    Thread.sleep(50000000)
  }

  @Test
  def testActions() = {

  }


  @After
  def after() {
    HDMContext.shutdown()
  }

}
