package org.hdm.core.functions

import org.junit.Test
import org.hdm.core.functions.CoGroupFunc

import scala.collection.mutable

/**
 * Created by tiantian on 28/03/16.
 */
class DualInputFunctionTest {

  val data1 =
    """
        this is a word count text
        this is line 2
        this is line 3
    """.split("\\s+").filter(_.nonEmpty).toIterator

  val data2 = Array(
    "a", "b", "c", "d", "e", "w", "l", "t"
  ).toIterator

  val data3 = Array.fill(100000){
    (Math.random() * 1000)
  }

  val data4 =  Array.fill(100000){
    (Math.random() * 10000)
  }

  @Test
  def testCoGroupFunction(): Unit ={
    val f1 = (w:String) => w.head.toString
    val func = new CoGroupFunc(f1, f1)
    val res = func((data1, data2))
    res.foreach(println(_))
  }

  @Test
  def testCoGroupAggregation(): Unit ={
    val groupFunc = (d:Double) => d.toInt % 100
    val func = new CoGroupFunc(groupFunc, groupFunc)
    var res = mutable.Buffer.empty[(Int,(Iterable[Double], Iterable[Double]))]
    val start = System.currentTimeMillis()
    for( i <- 1 to 10){
      res = func.aggregate((data3.toIterator, data4.toIterator), res)
    }

    val end = System.currentTimeMillis()
    res.foreach( kv => println(kv._1 + "," + kv._2._1.size + "," + kv._2._2.size))
    println(s"Time spent:${end - start} ms")

  }

  @Test
  def testCoGroupAggregator(): Unit ={
    val groupFunc = (d:Double) => d.toInt % 100
    val func = new CoGroupFunc(groupFunc, groupFunc)
    val start = System.currentTimeMillis()
    for( i <- 1 to 10){
      func.aggregate((data3.toIterator, data4.toIterator))
    }

    val end = System.currentTimeMillis()
    func.result.foreach( kv => println(kv._1 + "," + kv._2._1.size + "," + kv._2._2.size))
    println(s"Time spent:${end - start} ms")
  }

}
