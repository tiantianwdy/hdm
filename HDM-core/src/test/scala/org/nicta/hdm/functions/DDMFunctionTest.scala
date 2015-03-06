/*
package org.nicta.hdm.functions

import org.junit.Test
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.model.HDM
import org.nicta.wdy.hdm.functions._

/**
 * Created by Tiantian on 2014/12/11.
 */
class DDMFunctionTest {

  val text =
    """
        this is a word count text
        this is line 2
        this is line 3
    """.split("\\s+")

  val numberArray = Array.fill(10){Math.random()}

  @Test
  def testMapFunc(){
     val ddm = HDM(text)
     new MapFunc[String, Int]((d) => d match {
       case s:String  => s.length
       case _ => 0
     }).apply(Seq(ddm)).sample(10).foreach(println(_))
  }

  @Test
  def testGroupByFunc(){
    val ddm = HDM(text)
    val f = (d:String) => d match {
      case s if(s.length >= 3)=> s.substring(0,3)
      case ss => ss
    }

//    ddm.elems.groupBy(f).foreach(println(_))
    val res = new GroupByFunc[String,String](f).apply(Seq(ddm))
    res.sample().foreach(println(_))
  }

  @Test
  def testReduceFunc(){
    val f = (d:String) => d match {
      case s if(s.length >= 3)=> s.substring(0,3)
      case ss => ss
    }
    val ddm = HDM(text)

    val grouped = new GroupByFunc[String,String](f).apply(Seq(ddm))
    val res = new ReduceFunc[(String, Seq[String]), (String, Seq[String])]((s1,s2) => (s1._1, s1._2 ++ s2._2)).apply(Seq(grouped))
    res.sample().foreach(println(_))
  }

  @Test
  def testReduceByKeyFunc(){
    val ddm = HDM(text)
    val mapped = new MapFunc[String,(String,Int)]((_, 1)).apply(Seq(ddm))
    val res = new ReduceByKey[(String,Int), String](_._1, (s1,s2) => (s1._1, s1._2 + s2._2)).apply(Seq(mapped))
    res.sample().foreach(println(_))
  }

  @Test
  def testGroupFoldByKey(){

    val ddm = HDM(text)
    val mapped = new MapFunc[String,(String,Int)]((_, 1)).apply(Seq(ddm))
    val res = new GroupFoldByKey[(String,Int),String, Int](_._1, _._2, _+_).apply(Seq(mapped))
    res.sample().foreach(println(_))

  }

}
*/
