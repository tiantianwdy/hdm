package org.nicta.hdm.functions

import org.junit.Test

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParSeq

/**
 * Created by tiantian on 2/01/15.
 */
class CollectionEfficiencyTest {

  def generateData(num:Int, strLen:Int): Array[(String, String)] ={
    Array.fill(num){
      (Math.random()* num).toInt.toString -> 1.toString
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

    val arr = generateData(300000, 3)
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

}
