package org.hdm.core.model

import org.hdm.core.collections.BufUtils

import scala.collection.mutable
import org.junit.Test

/**
 * Created by tiantian on 7/06/15.
 */
class BufferUntilTest {

  def generateTuple(num:Int, range:Int): Array[(String, Double)] ={
    Array.fill(num){
      (Math.random()* range).toInt.toString -> 1D
    }
  }

  def genrateSeq(seqNum:Int, elemNum:Int): Seq[Seq[String]] = {
    for(sn <- 1 to seqNum) yield {
      Seq.fill(elemNum){
        "abs"
      }
    }
  }

  @Test
  def testBufferEfficiency(): Unit ={
    var res = mutable.Buffer.empty[(String, Double)]
    val c1 = generateTuple(10000,100).toBuffer
    val start = System.currentTimeMillis()
    for(i <- 1 to 100){
     res =  BufUtils.combine(res, c1)
    }
    val end = System.currentTimeMillis() -start
    println(s"finished in $end ms.. get ${res.length}")

  }

  @Test
  def testBufferAddEfficiency(): Unit ={
    var res = mutable.Buffer.empty[(String, Double)]
    val c1 = generateTuple(100000,100).toBuffer
    val start = System.currentTimeMillis()
    for(i <- c1){
      res =  BufUtils.add(res, i)
    }
    val end = System.currentTimeMillis() -start
    println(s"finished in $end ms.. get ${res.length}")

  }


  @Test
  def testArrayEfficiency(): Unit ={
    var res = Array.empty[(String, Double)]
    val c1 = generateTuple(10000,100)
    val start = System.currentTimeMillis()
    for(i <- 1 to 100){
      res =  BufUtils.combine(res, c1)
    }
    val end = System.currentTimeMillis() -start
    println(s"finished in $end ms.. get ${res.length}")

  }

  @Test
  def testSeqEfficiency(): Unit ={
    var res = Seq.empty[(String, Double)]
    val c1 = generateTuple(10000,100).toSeq
    val start = System.currentTimeMillis()
    for(i <- 1 to 100){
      res =  BufUtils.combine(res, c1)
//      res = res ++ c1
    }
    val end = System.currentTimeMillis() -start
    println(s"finished in $end ms.. get ${res.length}")

  }

  @Test
  def testSeqAddEfficiency(): Unit ={
    var res = Seq.empty[(String, Double)]
    val c1 = generateTuple(100000,100).toSeq
    val start = System.currentTimeMillis()
    for(i <- c1){
      res =  BufUtils.add(res, i)
    }
    val end = System.currentTimeMillis() -start
    println(s"finished in $end ms.. get ${res.length}")

  }

}
