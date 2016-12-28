
package org.hdm.core.functions

import org.junit.Test
import org.hdm.core.functions._

/**
 * Created by Tiantian on 2014/12/11.
 */
class DDMFunctionTest {

  val text =
    """
        this is a word count text
        this is line 2
        this is line 3
    """.split("\\s+").toIterator

  val numberArray = Array.fill(10){Math.random()}

  @Test
  def testMapFunc(){
//     val ddm = HDM(text)
     new ParMapFunc[String, Int]((d) => d match {
       case s:String  => s.length
       case _ => 0
     }).apply(text).foreach(println(_))
  }

  @Test
  def testGroupByFunc(){
//    val ddm = HDM(text)
    val f = (d:String) => d match {
      case s if(s.length >= 3)=> s.substring(0,3)
      case ss => ss
    }

//    ddm.elems.groupBy(f).foreach(println(_))
    val res = new ParGroupByFunc[String,String](f).apply(text)
    res.foreach(println(_))
  }

  @Test
  def testReduceFunc(){
    val f = (d:String) => d match {
      case s if(s.length >= 3)=> s.substring(0,3)
      case ss => ss
    }
//    val ddm = HDM(text)

    val grouped = new ParGroupByFunc[String,String](f).apply(text)
    val res = new ParReduceFunc[(String, Iterable[String]), (String, Iterable[String])]((s1,s2) => (s1._1, s1._2 ++ s2._2)).apply(grouped)
    res.foreach(println(_))
  }

  @Test
  def testReduceByKeyFunc(){
//    val ddm = HDM(text)
    val mapped = new ParMapFunc[String,(String,Int)]((_, 1)).apply(text)
    val res = new ParReduceBy[(String,Int), String](_._1, (s1,s2) => (s1._1, s1._2 + s2._2)).apply(mapped)
    res.foreach(println(_))
  }

  @Test
  def testGroupFoldByKey(){

//    val ddm = HDM(text)
    val mapped = new ParMapFunc[String,(String,Int)]((_, 1)).apply(text)
    val res = new ParGroupByAggregation[(String,Int),String, Int](_._1, _._2, _+_).apply(mapped)
    res.foreach(println(_))

  }

}

