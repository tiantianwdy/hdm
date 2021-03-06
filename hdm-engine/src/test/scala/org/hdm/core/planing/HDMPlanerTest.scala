package org.hdm.core.planing

import org.hdm.core.executor.ClusterTestSuite
import org.hdm.core.io.Path
import org.hdm.core.model.HDM
import org.junit.{After, Before, Ignore, Test}

/**
 * Created by Tiantian on 2014/12/10.
 */
class HDMPlanerTest extends ClusterTestSuite {


  @Before
  def beforeTest(): Unit ={
    hDMEntry.init()
  }


  @Test
  def testWordCountPlan(){
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
    println(text.length)
//    text.foreach(println(_))
    val hdm = HDM.horizontal(appContext, hDMContext, text, text2)
    val wordCount = hdm.map(d=> (d,1))
      .groupBy(_._1).map(t => (t._1, t._2.map(_._2))).reduce((t1,t2) => (t1._1, t1._2))
    val wordCountOpti = new FunctionFusion().optimize(wordCount)
    val hdms = new StaticPlaner(hDMContext).plan(wordCountOpti ,2).physicalPlan
    hdms.foreach(println(_))
  }

  @Ignore("Require local HDFS.")
  @Test
  def testClusterPlanner(): Unit ={
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/1node/rankings")
    val hdm = HDM(path)
    val wordCount = hdm.map{ w =>
      val as = w.split(",")
      (as(0).substring(0,3), as(1).toInt)
    }
//      .groupReduce(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))
      .groupBy(_._1)
      //.findByKey(_.startsWith("a"))
      //.map(t => (t._1, t._2.map(_._2).reduce(_+_)))
//      hdm.map(d=> (d,1)).groupBy(_._1)
      //.map(t => (t._1, t._2.map(_._2))).reduce(("", Seq(0)))((t1,t2) => (t1._1, t1._2))

    hDMEntry.explain(wordCount, 1).physicalPlan.foreach(println(_))

/*    val wordCountOpti = new FunctionFusion().optimize(wordCount)

    StaticPlaner.plan(wordCountOpti, 4).foreach(println(_))*/

  }

  @Ignore("Require local HDFS.")
  @Test
  def testSortPlanner(): Unit ={
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/micro/rankings")
    val hdm = HDM(path)
    val topk = hdm.map{ w =>
      val as = w.split(",")
      as(1).toInt
    }.top(10)
    //.count()
    new StaticPlaner(hDMContext).plan(topk, 4).physicalPlan.foreach(println(_))

  }

  @Ignore("Require local HDFS.")
  @Test
  def testCachePlaner(): Unit ={
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings")
    val hdm = HDM(path)
    val cache = hdm.map{ w =>
      val as = w.split(",")
      as(1).toInt
    }.cache()

    val res = cache.reduce(_ + _)
    hDMEntry.explain(res, 1).physicalPlan.foreach(println(_))

  }

  @Ignore("Require local HDFS.")
  @Test
  def testCogroupPlanning(): Unit ={
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/partial/rankings")
    val hdm = HDM(path)
    val data1 = hdm.map{ w =>
      val as = w.split(",")
      (as(1).toInt, as(2))
    }

    val data2 = hdm.map{ w =>
      val as = w.split(",")
      (as(1).toInt, as(3))
    }

    val res = data1.cogroup(data2, (d1:(Int, String))  => d1._1, (d2:(Int, String)) => d2._1)
    print(System.currentTimeMillis())
//    HDMContext.planer.logicalPlanner.plan(res, 1) foreach(println(_))
//    HDMContext.explain(res, 1).foreach(println(_))
  }

  @After
  def afterTest(): Unit ={
    hDMEntry.shutdown(appContext)
  }
}
