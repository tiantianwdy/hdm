package org.nicta.hdm.math

import breeze.linalg.{DenseVector, norm => bNorm}
import org.junit.Test
import org.nicta.wdy.hdm.model.HDM
import org.nicta.wdy.hdm.math.HDMatrix.{hdmToMatrix}

import scala.util.Random

/**
 * Created by tiantian on 30/11/16.
 */
class HDMatrixTest extends HDMathTestSuite {

  val numColumn = 100
  val numRows = 100
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
    val matrix = HDM.parallelize(elems = matrixData, numOfPartitions = 8).zipWithIndex.cache()
    // test map row
    printData(matrix.norm(parallelism))

  }

  @Test
  def testMatrixOps(): Unit = {

  }

}
