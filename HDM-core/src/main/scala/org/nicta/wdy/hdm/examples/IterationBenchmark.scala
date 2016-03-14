package org.nicta.wdy.hdm.examples

import breeze.linalg.{squaredDistance, Vector, DenseVector}
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.executor.HDMContext._
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
    val hdm = HDM(path).cached(parallelism)
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
    val hdm = HDM(path).cached(parallelism)
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
    val hdm = HDM(path).cache
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
    var start = System.currentTimeMillis()

    val hdm = HDM(path).map{ w =>
      val as = w.split(",")
      val arr = Array(as(kOffset), as(vOffset)).map(_.toDouble)
      DataPoint(Vector(arr), arr(0))
    }.cache


    for(i <- 1 to iterations) {
      val w = weights
//      val redFunc = (d1:Vector[Double], d2: Vector[Double]) => d1 + d2
      val gradient = hdm.map{ p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _).collect().next()
      weights -= gradient
      val end = System.currentTimeMillis()
      println(s"Time consumed for iteration $i : ${end - start} ms. weights: $weights")
      start = System.currentTimeMillis()
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


  def testWeatherLR(dataPath: String, vectorLen: Int, iteration:Int, p:Int = 4, cached:Boolean) = {
    implicit  val parallelism = p
    val path = Path(dataPath)
    val rand = new Random(42)
    val vecLen = vectorLen
    var weights = DenseVector.fill(vecLen){2 * rand.nextDouble() - 1}
    var start = System.currentTimeMillis()

    val data = HDM(path).map(line => line.split("\\s+"))
//      .filter(arr => arr.length > vecLen)
      .map{ seq => seq.drop(3).dropRight(6)}
      .filter(seq => seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{seq => seq.take(vecLen).map(_.toDouble).toArray}
      .map{arr =>
      val vec = Vector(arr)
      DataPoint(vec, arr(0))
    }
    val training = if(cached) data.cached else data.cache()

    for(i <- 1 to iteration) {
      val w = weights
      val gradient = training.map{ p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _).collect().next()
      weights -= gradient
      val end = System.currentTimeMillis()
      println(s"Time consumed for iteration $i : ${end - start} ms. weights: $weights")
      start = System.currentTimeMillis()
    }
    weights
  }


  def testWeatherKMeans(dataPath: String, vectorLen: Int, iterations:Int, p:Int = 4, K:Int, cached:Boolean) = {

    implicit  val parallelism = p
    val path = Path(dataPath)
    val vecLen = vectorLen
    var tempDist = 1.0
    var start = System.currentTimeMillis()

    val data = HDM(path).map(line => line.split("\\s+"))
      //      .filter(arr => arr.length > vecLen)
      .map{ seq => seq.drop(3).dropRight(6)}
      .filter( seq => seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{ seq => seq.take(vecLen).map(_.toDouble).toArray}
      .map{ arr => Vector(arr)}

    val training = if(cached) data.cached else data.cache()
    val kPointsBuffer = training.sample(K, 5000000).toArray
    println(s"Initial points: ${kPointsBuffer.length}")

    for(i <- 1 to iterations) {
      val kPoints = kPointsBuffer
//      println(s"Initial points: ${kPoints.take(2).mkString}")
      val closest = training.map (p => (closestPoint(p, kPoints), (p, 1)))
      val pointStats = closest.reduceByKey{case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)}
      val newPoints = pointStats.map {pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collect().toMap
      val oldPoints = kPoints.clone()
      for (newP <- newPoints) {
        kPointsBuffer(newP._1) = newP._2
      }

      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(oldPoints(i), kPointsBuffer(i))
      }

      val end = System.currentTimeMillis()
      println(s"Finished iteration $i  (delta = $tempDist ) in ${end - start} ms.")
      start = System.currentTimeMillis()
    }
    tempDist

  }

  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

}
