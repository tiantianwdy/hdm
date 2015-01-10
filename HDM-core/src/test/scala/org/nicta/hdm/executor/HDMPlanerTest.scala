package org.nicta.hdm.executor

import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.HDM
import org.junit.Test
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.planing.StaticPlaner

/**
 * Created by Tiantian on 2014/12/10.
 */
class HDMPlanerTest {

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
    HDMContext.init()
    val hdm = HDM.horizontal(text, text2)
    val wordCount = hdm.map(d=> (d,1))
      //.groupBy(_._1).map(t => (t._1, t._2.map(_._2))).reduce(("",Seq(0)))((t1,t2) => (t1._1, t1._2))
    val hdms = StaticPlaner.plan(wordCount ,1)
    hdms.foreach(println(_))
  }

  @Test
  def testClusterPlanner(): Unit ={
    HDMContext.init()
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/micro/rankings")
    val hdm = HDM(path, false)
    val wordCount = hdm.map{ w =>
      val as = w.split(",");
      (as(0).substring(0,3), as(1).toInt)
    }.groupBy(_._1)
      //hdm.map(d=> (d,1)).groupBy(_._1)
      //.map(t => (t._1, t._2.map(_._2))).reduce(("", Seq(0)))((t1,t2) => (t1._1, t1._2))
    StaticPlaner.plan(wordCount, 4).foreach(println(_))

  }

  @Test
  def testSortPlanner(): Unit ={
    HDMContext.init()
    val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/micro/rankings")
    val hdm = HDM(path)
    val topk = hdm.map{ w =>
      val as = w.split(",");
      as(1).toInt
    }.top(10)
    //.count()
    StaticPlaner.plan(topk, 4).foreach(println(_))

  }

}
