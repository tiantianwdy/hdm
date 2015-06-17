package org.nicta.hdm.functions

import org.junit.Test
import org.nicta.wdy.hdm.Buf
import org.nicta.wdy.hdm.functions.{ParGroupByFunc, ParReduceBy, ParReduceFunc}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParSeq

/**
 * Created by tiantian on 2/01/15.
 */
class CollectionEfficiencyTest {

  val data = ArrayBuffer.fill[(String, Double)](1000000){
    ("0xb601998146d35e06", 1D)
  }

  def generateTuple(num:Int, range:Int): Array[(String, String)] ={
    Array.fill(num){
      (Math.random()* range).toInt.toString -> 1.toString
    }
  }

  def genrateSeq(seqNum:Int, elemNum:Int): Seq[Seq[String]] = {
    for(sn <- 1 to seqNum) yield {
      Seq.fill(elemNum){
        "abs"
      }
    }
  }

  def testMapFunction(): Unit ={

  }

  @Test
  def testReduceFunction(): Unit ={
    //test Array

    val arr = generateTuple(300000, 3)
    val collection = arr.toSeq
    val start = System.currentTimeMillis()
//    val res = arr.reduce((d1,d2) => (d1._1, d1._2 + d2._2))

//    val res = collection.reduce((d1,d2) => (d1._1, d1._2 + d2._2))

    val res = collection.groupBy(_._1).toSeq
    for ( seq <- res ) yield {
      val k = seq._1
      val v = ArrayBuffer.empty[String]
      for (e <- seq._2){
        v += e._2
      }
      (k,v.toSeq)
    }
    val end = System.currentTimeMillis() - start
    println(s"Finished in $end ms: ${res.take(10)}")
  }

  @Test
  def testCollectionContact(): Unit ={
    val collection = genrateSeq(300000,1)
    val start = System.currentTimeMillis()
    //test default seq op
    val res = collection.reduce((s1,s2) => ArrayBuffer.empty[String] ++= s1 ++= s2)
//    val res = collection.flatten
/*    val res = ArrayBuffer.empty[String]
    for (c <- collection) {
      res ++= c
    }*/

    val end = System.currentTimeMillis() - start
    println(s"Finished in $end ms: ${res.take(10)}")
  }
  
  @Test
  def testReduceByFunc(): Unit ={
    val start = System.currentTimeMillis()
    val collection = generateTuple(300000,10000).toBuffer
    val f = (d1:(String, String), d2:(String,String)) => (d1._1, (d1._2.toInt + d2._2.toInt).toString)
    val func =  new ParReduceBy[(String,String), String](_._1, f)
    val res = func.apply(collection)
    val end = System.currentTimeMillis() - start
    println(s"Finished in $end ms: ${res.take(10)}")
  }

  @Test
  def testReduceByAggregation(): Unit ={
    val start = System.currentTimeMillis()
    val iterNum = 10
    val collection = generateTuple(300000,10000).toBuffer
    val f = (d1:(String, String), d2:(String,String)) => (d1._1, (d1._2.toInt + d2._2.toInt).toString)
    val func =  new ParReduceBy[(String,String), String](_._1, f)
    var res = Buf.empty[(String, (String, String))]
    for( i <- 0 until iterNum)
     res =  func.aggregate(collection, res)
    val end = System.currentTimeMillis() - start
    println(s"Finished in $end ms: ${res.take(10)}")
  }

  @Test
  def testGroupByAggregation(): Unit ={

    val iterNum = 10
    val collection = generateTuple(1000000,300000).toBuffer
    val start = System.currentTimeMillis()
    val func =  new ParGroupByFunc[(String,String), String](_._1)
//    var res = mutable.Map.empty[String, scala.collection.mutable.Buffer[(String, String)]]
//    var res = mutable.Buffer.empty[(String, scala.collection.mutable.Buffer[(String, String)])]
    var res = Buf.empty[(String, Buf[(String, String)])]
    for( i <- 0 until iterNum)
      res =  func.aggregate(collection, res)
    val end = System.currentTimeMillis() - start
    println(s"Finished in $end ms: ${res.take(10).mkString("\n")}")
  }

  @Test
  def testMultipleFunctionEfficiency(): Unit ={
    val f = (d:(String,Double)) => (d._1.substring(2), d._2)
    val seq = data
    val start = System.currentTimeMillis()
    val result = seq.map{d => (d._1.substring(2), d._2)}
      .map{d => (d._1.substring(2), d._2)}
      .map{d => (d._1.substring(2), d._2)}
      .map{d => (d._1.substring(2), d._2)}
//      .map{d => (d._1.substring(0, 3), d._2)}
    println(s"finished in ${result.take(10)} ms")
    val end = System.currentTimeMillis() - start
    println(s"finished in $end ms")

  }

  @Test
  def testEmbededMuliFunction(): Unit ={
    val f = (d:(String,Double)) => (d._1.substring(2), d._2)
    val seq = data
    val start = System.currentTimeMillis()
    val result = seq.map{f.andThen(f).andThen(f).andThen(f)}
//      .map{d => (d._1.substring(0, 3), d._2)}
    println(s"finished in ${result.take(10)} ms")
    val end = System.currentTimeMillis() - start
    println(s"finished in $end ms")
  }
}
