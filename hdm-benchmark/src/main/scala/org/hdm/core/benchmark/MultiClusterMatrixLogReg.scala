package org.hdm.core.benchmark

import breeze.linalg.DenseVector
import org.hdm.core.io.Path
import org.hdm.core.math.{VectorOps, HDVector, HDMRowMatrix}
import org.hdm.core.model.HDM

import scala.util.Random
import org.hdm.core.math.HDMRowMatrix._
import VectorOps.hdmToVector

/**
  * Created by tiantian on 25/09/17.
  */
class MultiClusterMatrixLogReg(master1:String,
                               master2:String
                               )(implicit parallelism:Int) extends MultiClusterBenchmark(master1, master2) {

  /**
    *
    * @param dataPath1
    * @param dataPath2
    * @param vectorLen
    * @return
    */
  def fromCSV(dataPath1:String, dataPath2:String, vectorLen: Int):(HDMRowMatrix[Double], HDMRowMatrix[Double], HDVector[Double]) = {
    hDMEntry.init(leader = master1, slots = 0)
    Thread.sleep(200)

    val vecLen = vectorLen / 2
    var weights = DenseVector.fill(vectorLen){0.01 * Random.nextDouble()}
    val data1 = Path(dataPath1)
    val data2 = Path(dataPath2)
    val dataDP1 = HDM(data1, appContext1)
    val dataDP2 = HDM(data2, appContext2)

    val trainingDp1 = dataDP1.map(line => line.split("\\s+"))
      .map{ seq => seq.drop(3).dropRight(6)}
      .filter(seq => seq.size >= 6 && seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{seq => DenseVector(seq.take(vecLen).map(_.toDouble))}
      .zipWithIndex.cache()


    val trainingDp2 = dataDP2.map(line => line.split("\\s+"))
      .map{ seq => seq.drop(3).dropRight(6)}
      .filter(seq => seq.size >= 6 && seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{seq => DenseVector(seq.takeRight(vecLen).map(_.toDouble))}
      .zipWithIndex

    val labels = trainingDp1.map(tup => (tup._1 -> tup._2(0))).cache()

    (trainingDp1, trainingDp2, new HDVector[Double](labels))
  }


  /**
    *
    * @param dataPath1
    * @param dataPath2
    * @param numColumn
    * @param numIteration
    */
  def runTwoClusterMatrixLR(dataPath1:String,
                              dataPath2:String,
                              numColumn: Int,
                              numIteration: Int): Unit ={
    val (m1, m2, labels) = fromCSV(dataPath1, dataPath2, numColumn)
    val x = HDMRowMatrix.vertical(m1, m2).cache()
    val y = labels
    val weights = DenseVector.fill(numColumn){0.01 * Math.random()}

    for(i <- 1 to numIteration){
      val start = System.currentTimeMillis()
      val gradient = y.minus(x.dot(weights).sigmoid()) *: x
      weights -= gradient
      val end = System.currentTimeMillis()
      println(s"Iteration $i finished in ${end - start} ms.")
      println(weights)
    }

  }

}
