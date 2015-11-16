package org.nicta.wdy.hdm.benchmark

import breeze.linalg.{Vector, DenseVector}
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.HDM

import scala.math._
import scala.util.Random

/**
 * Created by tiantian on 8/10/15.
 */
class IterationBenchmark(val kIndex:Int = 0, val vIndex:Int = 1)  extends Serializable{

  def init(context:String, localCores:Int = 0): Unit ={
    HDMContext.init(leader = context, slots = localCores)
    Thread.sleep(100)
  }


  def testGeneralIteration(dataPath:String, parallelism:Int = 4) = {
    val path = Path(dataPath)
    val hdm = HDM(path).cache(parallelism)
    for(i <- 1 to 3){
      val start = System.currentTimeMillis()
      val vOffset = 1 // variables are only available in this scope , todo: support external variables
      hdm.map{ w =>
        val as = w.split(",")
        as(vOffset).toFloat + i*100
      }.collect()(parallelism).take(20).foreach(println(_))
      val end = System.currentTimeMillis()
      println(s"Time consumed for iteration $i : ${end - start} ms.")
      Thread.sleep(100)
    }

  }

  def testIterationWithAggregation(dataPath:String, parallelism:Int = 4): Unit ={
    val path = Path(dataPath)
    val hdm = HDM(path).cache(parallelism)
    //    val kOffset = 0
    var aggregation = 0F
    for(i <- 1 to 3){
      val start = System.currentTimeMillis()
      val vOffset = 1 // only avaliable in this scope
      val agg = aggregation // todo: support external variables by editing closure cleaner
      val res = hdm.map{ w =>
        val as = w.split(",")
        as(vOffset).toFloat + i*agg
      }.collect()(parallelism).take(20).toSeq
      aggregation += res.sum

      val end = System.currentTimeMillis()
      res.foreach(println(_))
      println(res.sum)
      println(s"Time consumed for iteration $i : ${end - start} ms. aggregation: $aggregation")
      Thread.sleep(100)
    }
  }


  def testIterationWithCache(dataPath:String, parallelism:Int = 4)={

    val path = Path(dataPath)
    val hdm = HDM(path).cache(parallelism)
    var aggregation = 0F

    for(i <- 1 to 3) {
      val start = System.currentTimeMillis()
      val vOffset = 1 // only avaliable in this scope, todo: support external variables
      val agg = aggregation
      val res = hdm.map{ w =>
        val as = w.split(",")
        as(vOffset) + i*agg
      }.collect()(parallelism).take(20).toSeq
      res.foreach(println(_))
//      aggregation += res.sum
      val end = System.currentTimeMillis()
      println(s"Time consumed for iteration $i : ${end - start} ms. aggregation: $aggregation")
      Thread.sleep(100)
    }

  }

  case class DataPoint(x: Vector[Double], y: Double) extends Serializable

  def testLinearRegression(dataPath:String, iterations:Int, p:Int = 4) = {
    implicit  val parallelism = p
    val path = Path(dataPath)
    val kOffset = kIndex
    val vOffset = vIndex
    val rand = new Random(42)
    var weights = DenseVector.fill(2){2 * rand.nextDouble - 1}

    val hdm = HDM(path).map{ w =>
      val as = w.split(",")
      val arr = Array(as(kOffset), as(vOffset)).map(_.toDouble)
      DataPoint(Vector(arr), arr(0))
    }.cache


    for(i <- 1 to iterations) {
      val start = System.currentTimeMillis()
      val w = weights
//      val redFunc = (d1:Vector[Double], d2: Vector[Double]) => d1 + d2
      val gradient = hdm.map{ p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _).collect().next()
      weights -= gradient
      val end = System.currentTimeMillis()
      println(s"Time consumed for iteration $i : ${end - start} ms. weights: $weights")
    }
  }


  def testLogisticRegression(dataPath: String, vecLen: Int, labelIdx: Int, iterations:Int, p:Int = 4)={
    implicit  val parallelism = p
    val path = Path(dataPath)
    val rand = new Random(42)
    var weights = DenseVector.fill(vecLen){2 * rand.nextDouble - 1}

    val hdm = HDM(path).map{ w =>
      val as = w.split(",")
      val arr = as.take(vecLen).map(_.toDouble)
      DataPoint(Vector(arr), arr(labelIdx))
    }.cache

    for(i <- 1 to iterations) {
      val start = System.currentTimeMillis()
      val w = weights
      //      val redFunc = (d1:Vector[Double], d2: Vector[Double]) => d1 + d2
      val gradient = hdm.map{ p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _).collect().next()
      weights -= gradient
      val end = System.currentTimeMillis()
      println(s"Time consumed for iteration $i : ${end - start} ms. weights: $weights")
    }

  }


  def testWeatherLR(dataPath: String, vectorLen: Int, iteration:Int, p:Int = 4) = {
    implicit  val parallelism = p
    val path = Path(dataPath)
    val rand = new Random(42)
    val vecLen = vectorLen
    var weights = DenseVector.fill(vecLen){2 * rand.nextDouble - 1}

    val training = HDM(path).map(line => line.split("\\s+"))
//      .filter(arr => arr.length > vecLen)
      .map{ seq => seq.drop(3).dropRight(6)}
      .filter(seq => seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{seq => seq.take(vecLen).map(_.toDouble).toArray}
      .map{arr =>
      val vec = Vector(arr)
      DataPoint(vec, arr(0))
    }.cache


    for(i <- 1 to iteration) {
      val start = System.currentTimeMillis()
      val w = weights
      val gradient = training.map{ p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _).collect().next()
      weights -= gradient
      val end = System.currentTimeMillis()
      println(s"Time consumed for iteration $i : ${end - start} ms. weights: $weights")
    }
    weights
  }


}
