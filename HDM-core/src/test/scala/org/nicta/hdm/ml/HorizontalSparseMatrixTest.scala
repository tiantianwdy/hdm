package org.nicta.hdm.ml

import org.nicta.unsw.ml.{HierarchyHorizontalMatrix, HorizontalSparseMatrix, DataParser}
import org.junit.Test

/**
 * Created by Dongyao.Wu on 2014/5/29.
 */
class HorizontalSparseMatrixTest {

  @Test
  def testBasicOp(){
    val path = "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\HOSPITAL_QUARTERLY_QUALITYMEASURE_IPFQR_HOSPITAL.csv"
    val indexes = Seq(9,10)
    val data = DataParser.parseCsv(path, "\",\"", 0, indexes)
    val matrix = new HorizontalSparseMatrix(indexes.length.toLong, data)
    matrix + 1 printData(10)
    println("===============================")
    matrix + 1 dot Seq(1D,2D) printData(10)
    println("===============================")
  }

  @Test
  def testAdvancedOp(){
    val data = Map(
      "1" -> Seq[Double](1,2,3),
      "2" -> Seq[Double](2,2,3),
      "3" -> Seq[Double](3,2,3),
      "4" -> Seq[Double](4,2,3),
      "5" -> Seq[Double](2,3,4)
    )
    val data2 = Map(
      "1" -> Seq[Double](1,2,3,4),
      "2" -> Seq[Double](2,2,3,4),
      "3" -> Seq[Double](3,2,3,4),
      "6" -> Seq[Double](4,2,3,5),
      "5" -> Seq[Double](2,3,4,6)
    )

    val mx = new HorizontalSparseMatrix(3,data)
    val mx2 = new HorizontalSparseMatrix(4,data2)

    mx + mx2 printData(0)
    mx - mx2 printData(0)
    mx * mx2 printData(0)
    mx / mx2 printData(0)


    println("===============================")
    val theta = Seq[Double](1,2,3)
    mx.dot(theta) printData(0)

    println("===============================")
    val vector = mx2.rExtract(2,3) printData(0)
    mx * vector printData(0)

    println("===============================")
    mx.rSum() printData(0)
    mx.cSum() printData(0)

    println("===============================")
    mx.cMax() printData(0)
    mx.cMin() printData(0)
    mx.cMax() - mx.cMin() printData(0)

    println("===============================")
    mx.cAvg() printData(0)
    mx.rAvg() printData(0)

    println("===============================")

    (mx - mx.cAvg()) / (mx.cMax() - mx.cMin()) printData(0)

    println("============err===================")
    mx.rExtract(to =1) - mx2.rExtract(3,4) printData(0)
    val err = mx.rExtract(to =1) - (mx2.rExtract(3,4),true)
    err printData(0)
  }

  @Test
  def testMatrixWholeOp(){
    val path = "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\HOSPITAL_QUARTERLY_QUALITYMEASURE_IPFQR_HOSPITAL.csv"
    val indexes = Seq(9,10)
    val data = DataParser.parseCsv(path, """,""", 0, indexes)
    val matrix1 = new HorizontalSparseMatrix(indexes.length.toLong, data)

    val path2 = "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Outpatient Imaging Efficiency Measures.csv"
    val indexes2 = 10 to 26
    val data2 = DataParser.parseCsv(path2, """,""", 0, indexes2)
    val matrix2 = new HorizontalSparseMatrix(indexes2.length.toLong, data2)

    matrix1.rAppend(2) printData (5)
    matrix1.rExtract(0,1) printData (5)
    println("===============================")
    println(matrix1.cLength())
    println("===============================")
    println(matrix1.cAppend(matrix2).cLength())
    println("===============================")
    println(matrix2.rLength())
    matrix1.cAppend(matrix2).printData(10)
    matrix2.cAppend(matrix1).printData(10)
    println("===============================")
    matrix1 - matrix1 printData(10)
    println("===============================")
    matrix1 - (matrix2+1) printData(10)
    println("===============================")
    val theta = Seq.fill(19){0.5D}
    println(theta)
    println("===============================")
    val hx= (matrix1 dot theta.take(2))  + (matrix2.rExtract(to = 16) dot theta.take(16))
    hx printData 10
    println("===============================")
    val y = matrix2(16)
    y printData 10
    println("===============================")
    val err = (hx - y) * (0.01/4500)
    err printData 10
    println("===============================")
    val delta = (matrix1 * err)
    val delta2 = (matrix2 * err)
    delta printData 100
    println("===============================")
    val grad = delta.cSum() rAppend (delta2.cSum())
    grad printData(0)
  }





}