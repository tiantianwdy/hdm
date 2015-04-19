package org.nicta.hdm.benchmark

import org.junit.{After, Test}
import org.nicta.wdy.hdm.benchmark.HDMPrimitiveBenchmark
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
    HDMContext.init(leader = "akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster")
    Thread.sleep(1000)
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings")
    val hdm = HDM(path, false)

    val wordCount = hdm.map{ w =>
      val as = w.split(",");
      (as(0).substring(0,3), as(1).toInt)
    }
      .groupBy(_._1)
      .findByKey(_.startsWith("s"))
      //.map(t => (t._1, t._2.map(_._2).reduce(_+_)))
//      .groupReduce(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))


    wordCount.compute(4) onComplete {
      case Success(hdm) =>
        println("Job completed and received response:" + hdm)
        hdm.asInstanceOf[HDM[_, _]].sample().foreach(println(_))
      case Failure(t) =>
        println("Job failed because of: " + t)
        t.printStackTrace()
    }

    Thread.sleep(50000000)
  }


  @Test
  def testBenchMark(): Unit ={
    val context = "akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster"
    val data = "hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings"
    val parallelism = 4
    val len = 3
    val benchmark = new HDMPrimitiveBenchmark(context)
    HDMContext.init(leader = "akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster")
    Thread.sleep(1500)
//    benchmark.testGroupBy(data,len, parallelism)
//    benchmark.testFindByKey(data,len, parallelism, "a")
    benchmark.testReduceByKey(data,len, parallelism)
//    benchmark.testMap(data,len, parallelism)
    Thread.sleep(50000000)
  }


  @After
  def after() {
    HDMContext.shutdown()
  }

}
