package org.nicta.hdm.ml

import org.junit.Test
import org.nicta.unsw.ml.{HorizontalSparseMatrix, HierarchyHorizontalMatrix}

/**
 * Created by Tiantian on 2014/5/30.
 */
class HierarchyHorizontalMatrixTest {

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
    "5" -> Seq[Double](2,3,3,6)
  )

  val mx = new HorizontalSparseMatrix(3,data)
  val mx2 = new HorizontalSparseMatrix(4,data2)
  val hmx = new HierarchyHorizontalMatrix(Seq(mx,mx2))
  val indexedHmx = new HierarchyHorizontalMatrix(Map("mx-1"-> mx, "mx-2"-> mx2))


  @Test
  def testBasicOp(){


    hmx + 1 printData(1)
    hmx - 1 printData(1)
    hmx * 2 printData(2)
    hmx / 2 printData(2)

    println("=========================")

    hmx + (hmx /2) printData(0)

    println("=========================")

    mx2.rRemove(2) printData(0)

    hmx + mx2.rExtract(2,3) printData(0)

    println("=========================")

    val theta = Seq.fill(7)(1D)
    hmx dot(theta) printData(0)
    hmx dot(theta.take(6)) printData(0)

    println("=========================")

    val mTheta = Map(
      "mx-1" -> Seq.fill(3)(1.5D),
      "mx-2" -> Seq.fill(4)(2D))
    indexedHmx dot(mTheta) printData(0)
    indexedHmx dot(mTheta.take(1)) printData(0)

  }

  @Test
  def testAdvancedOp(){

    mx2.rRemove(2) printData(0)
    println(indexedHmx.rLength)
    println(indexedHmx.rRemove("mx-1",1).rLength)

    hmx + mx2.rExtract(2,3) printData(0)

    println("=========================")

    val theta = Seq.fill(7)(1D)
    hmx dot(theta) printData(0)
    hmx dot(theta.take(6)) printData(0)

    println("=========================")

    val mTheta = Map(
      "mx-1" -> Seq.fill(3)(1.5D),
      "mx-2" -> Seq.fill(4)(2D))
    indexedHmx dot(mTheta) printData(0)
    indexedHmx dot(mTheta.take(1)) printData(0)
  }

  @Test
  def testTakePartition(){
    println(hmx.cLength)
    hmx.take(0,3, null) printData(0)
    println("=========================")
    val part = hmx.take(Seq(1,2), 3, null) printData(0)
    println(part.cLength)
  }


}
