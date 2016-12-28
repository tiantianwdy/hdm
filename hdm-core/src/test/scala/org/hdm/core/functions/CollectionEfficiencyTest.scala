package org.hdm.core.functions

import breeze.linalg.{Vector, DenseVector}
import org.junit.Test
import org.hdm.core.{Arr, Buf}
import org.hdm.core.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ParSeq
import scala.math._
import scala.util.Random

/**
 * Created by tiantian on 2/01/15.
 */
class CollectionEfficiencyTest {

  val data = ArrayBuffer.fill[(String, Double)](1000000){
    ("0xb601998146d35e06", 1D)
  }

  def generateTuple(num:Int, range:Int) ={
    Array.fill(num){
      (Math.random()* range).toInt.toString -> 1.toString
    }
  }.toIterator

  def genrateSeq(seqNum:Int, elemNum:Int): Seq[Seq[String]] = {
    for(sn <- 1 to seqNum) yield {
      Seq.fill(elemNum){
        "abs"
      }
    }
  }

  def genrerateVector(num:Int, range:Int) ={
    val rand = new Random(42)
    Array.fill(num){
      DenseVector.fill(2){2 * rand.nextDouble - 1}
    }
  }.toIterator

  def genrerateDataPoint(num:Int, range:Int) ={
    val rand = new Random(42)
    Array.fill(num){
      val vec = DenseVector.fill(2){2 * rand.nextDouble - 1}
      DataPoint(vec, vec(0))
    }
  }.toIterator

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

    val collection = generateTuple(300000,10000)
    val f = (d1:(String, String), d2:(String,String)) => (d1._1, (d1._2.toInt + d2._2.toInt).toString)
    val func =  new ParReduceBy[(String,String), String](_._1, f)
    val start = System.currentTimeMillis()
    val res = func.apply(collection)
    val end = System.currentTimeMillis() - start
    println(s"Finished in $end ms: ${res.take(10)}")
  }

  case class DataPoint(x: Vector[Double], y: Double) extends Serializable

  @Test
  def testReduceFunc(): Unit ={

    val collection = genrerateVector(3000000, 10000)


    val reduce = (d1:Vector[Double], d2:Vector[Double]) => d1 + d2
    val func =  new ParReduceFunc[Vector[Double], Vector[Double]](reduce)
    val start = System.currentTimeMillis()
    val res = func.apply(collection)
    val end = System.currentTimeMillis() - start
    println(s"Finished in $end ms: ${res.take(10)}")
  }

  @Test
  def testReduceCombinedFunc(): Unit ={
    val rand = new Random(42)
    val w = DenseVector.fill(2){2 * rand.nextDouble - 1}
    val collection = genrerateDataPoint(2160000, 10000).toBuffer
    val collectionIter = collection.iterator

    val map = (p:DataPoint) => p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
    val mapFunc = new ParMapFunc[DataPoint, Vector[Double]](map)
    val reduce = (d1:Vector[Double], d2:Vector[Double]) => d1 + d2
    val reduceFunc =  new ParReduceFunc[Vector[Double], Vector[Double]](reduce)
    val union = new ParUnionFunc[DataPoint]

//    val start3 = System.currentTimeMillis()
//    val combined = mapFunc.andThen(reduceFunc)
//    val optRes = combined(collection.iterator)
//    val end3 = System.currentTimeMillis()
//    println(s"Finished Combined in ${end3 - start3} ms: ${optRes.next()}")

    val start = System.currentTimeMillis()
    val reduceInput = mapFunc.apply(collection.iterator)
//    val end = System.currentTimeMillis()
//    println(s"Finished Map in ${end - start} ms: ${reduceInput.take(10)}")
    val res = reduceFunc.apply(reduceInput)
    val end2 = System.currentTimeMillis()
    println(s"Finished Reduce in ${end2 - start} ms: ${res.next}")

//    val start0 = System.currentTimeMillis()
//    val reduceInputItr = collection.iterator.map(map)
//    val res = reduceInputItr.reduce(reduce)
//    val size = reduceInputItr.size
//    val end0 = System.currentTimeMillis()
//    println(s"Finished Map iterator in ${end0 - start0} ms: ${res}")
  }


  @Test
  def testReduceByAggregation(): Unit ={
    val start = System.currentTimeMillis()
    val iterNum = 10
    val collection = generateTuple(300000,10000)
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
    val collection = generateTuple(1000000,300000)
    val start = System.currentTimeMillis()
    val func =  new ParGroupByFunc[(String,String), String](_._1)
//    var res = mutable.Map.empty[String, scala.collection.mutable.Buffer[(String, String)]]
//    var res = mutable.Buffer.empty[(String, scala.collection.mutable.Buffer[(String, String)])]
    var res = Buf.empty[(String, Iterable[(String, String)])]
    for( i <- 0 until iterNum)
      res =  func.aggregate(collection, res)
    val end = System.currentTimeMillis() - start
    println(s"Finished in $end ms: ${res.take(10).mkString("\n")}")
  }

  @Test
  def testMultipleFunctionEfficiency(): Unit ={
    val f = (d:(String,Double)) => (d._1.substring(1), d._2)
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
  def testCombinedFunction(): Unit ={
    val init = System.currentTimeMillis()
    val collection = generateTuple(5000000, 10000)
    val f = (d:(String, String)) => (d._1.substring(1), d._2)
    val f2 = (d:(String, String)) => (d._1, d._2.toDouble)
    val f3 = (d:(String, Double)) => (d._1, d._2 * 20.5D)
    val f4 = (d:(String, Double)) => (d._1, d._2 * 20.5D)
    val mapF = new ParMapFunc(f)
    val mapF2 = new ParMapFunc(f2)
    val mapF3 = new ParMapFunc(f3)
    val mapF4 = new ParMapFunc(f4)
    val combinedFunc = mapF.andThen(mapF2).andThen(mapF3).andThen(mapF4)
//    val chainedFUnc = mapF.chain(mapF2).chain(mapF3).chain(mapF4)
    val start = System.currentTimeMillis()
    val result = combinedFunc.apply(collection)
//      .map{d => (d._1.substring(0, 3), d._2)}
    println(s"finished with size ${result.size}")
    val end = System.currentTimeMillis()
    println(s"func finished in ${end - start} ms")
    println(s"finished in total ${end - init} ms")
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
