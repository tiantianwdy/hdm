package org.nicta.unsw.ml

import scala.collection.parallel.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by Dongyao.Wu on 2014/6/5.
 */
object LinearRegressionFunc extends Func{
  
  type HMX = HierarchyHorizontalMatrix
  type SMX = HorizontalSparseMatrix

  def linearRegression(x : HierarchyHorizontalMatrix, y :HorizontalSparseMatrix, cvX:HierarchyHorizontalMatrix, cvY:HorizontalSparseMatrix, alpha: Double, iterNum:Int, reg:Boolean = false ) ={

    val n = x.rLength
    val m = y.cLength
    // weights initialization
    var theta = Seq.fill(n.toInt){Math.random()}
    var results = List.empty[(Double,Double,Double,Seq[Double])]
    // do regularization
    val regX = if(reg) featureScale(x) else x
    for (ite <- 1 to iterNum){
      val hx = regX.dot(theta)
      val err = hx - y
      val grad = (regX * err).cSum / m
      val gradValue = grad.collect().apply("_total")
      val cost =  DoubleFunc.opTwoSeq(gradValue, gradValue, DoubleFunc.multiply).sum / m
      val delta = gradValue.map(_ * alpha)
      theta = DoubleFunc.opTwoSeq(theta, delta, DoubleFunc.minus)
      //        println("cost:" + cost)
      val sqrtErr = predictError(regX, y, theta)._1
      val cvErr =   predictError(cvX, cvY, theta)._1
      results =  results :+ (cost, sqrtErr, cvErr,  theta)
    }
    results
  }

  def predictError(x: HierarchyHorizontalMatrix, y:HorizontalSparseMatrix, theta:Seq[Double]) = {
    val err = (x dot theta) - (y,true)
    (Math.sqrt(err.apply(e => Math.pow(e,2)).cAvg().collect().apply("_total").apply(0)), err)
  }

  def featureScale(x : HierarchyHorizontalMatrix) = x.map(mx => {(mx - mx.cAvg) / (mx.cMax - mx.cMin)})


  def getCVDataSet(x :HMX, y:SMX, folds:Int = 3): Seq[(HMX,SMX,HMX,SMX)] = {
     for (part <- 0 until folds) yield {
       val trainPart = (0 until folds).toList.filter(_ != part)
       val trainX = x.take(trainPart , folds, null)
       val trainY = y.take(trainPart, folds, null)
       val cvX = x.take(Seq(part),folds,null)
       val cvY = y.take(Seq(part),folds,null)
       (trainX,trainY,cvX,cvY)
     }
  }

}
