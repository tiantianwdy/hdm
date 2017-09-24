package org.hdm.core.math

import breeze.linalg.{DenseVector}
import org.hdm.core.math.HDMRowMatrix._

import org.hdm.core.model.HDM
import org.junit.Test

import scala.util.Random

/**
 * Created by tiantian on 30/11/16.
 */
class HDMatrixTest extends HDMathTestSuite {


  val matrixData = Seq.fill[DenseVector[Double]](numRows) {
    val vec = Array.fill[Double](numColumn) {
      Random.nextDouble()
    }
    DenseVector.apply(vec)
  }



  @Test
  def testPrimitives(): Unit = {
    val matrix = HDM.parallelize(elems = matrixData, numOfPartitions = 8).zipWithIndex.cache()

    // test map row
    printData(matrix.mapRow(vec => vec + 5D))
    //
    printData(matrix.mapElem(e => e + 10D))
    //
    printData(matrix.sumColumn())
  }

  @Test
  def testReduceRow(): Unit = {
    val matrix = HDM.parallelize(elems = matrixData, numOfPartitions = 8).zipWithIndex.cache()
    println(matrix.mapElem(v => v * v).reduceRow((v1, v2) => v1 + v2))
  }


  @Test
  def testNorm(): Unit = {
    val matrix = HDM.parallelWithIndex(elems = matrixData, numOfPartitions = 8).cache()
    // test map row
    printData(matrix.norm)

  }

  @Test
  def testMatrixZipMap(): Unit = { //todo to be fixed
    val matrix = HDM.parallelize(elems = matrixData, numOfPartitions = 8).zipWithIndex.cache()
    val vector = HDM.parallelize(elems = vecData, numOfPartitions = 8).zipWithIndex

    val res = matrix.zipMap(vector, (t1, t2) => t1 * t2).reduceRow((v1, v2) => v1 + v2)

//    printData(res)
    println(res)

  }

  @Test
  def testLogisticRegression(): Unit ={
    val matrix:HDMRowMatrix[Double] = HDM.parallelWithIndex(elems = matrixData, numOfPartitions = 4).cache()
    val y = new HDVector[Double](HDM.parallelWithIndex(elems = vecData, numOfPartitions = 4).cache())
    val weights = DenseVector.fill(numColumn){0.01 * Math.random()}


    import VectorOps.hdmToVector

    val hx = y.minus(matrix.dot(weights).sigmoid())
    val gradient = matrix.zipMap(hx.self, (vec, d) => vec * d).map(_._2).reduce(_ + _).collect().next()
//    val gradient = matrix.times(hx)
    weights -= gradient
    println(weights)

  }

}
