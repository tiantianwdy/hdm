package org.nicta.hdm.ml

import org.junit.Test
import scala.io.Source
import java.nio.file.{StandardOpenOption, Paths, Path, Files}
import org.nicta.unsw.ml.{HorizontalSparseMatrix, DoubleFunc, HierarchyHorizontalMatrix}
import org.nicta.unsw.ml.LinearRegressionFunc._

/**
 * Created by Dongyao.Wu on 2014/6/2.
 */
class AlgorithmTests {

  val pathMap = Seq(
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Healthcare_Associated_Infections.csv" -> Seq(11),
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\HOSPITAL_QUARTERLY_QUALITYMEASURE_IPFQR_HOSPITAL.csv" ->Seq(8,9),
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Immunization.csv" -> Seq(12),
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Medicare hospital spending per patient.csv" -> Seq(11),
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Medicare Volume Measures.csv" -> Seq(11),
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Outcome of Care Measures.csv" -> (11 to 64),
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Outpatient Imaging Efficiency Measures.csv" -> (11 to 26)
  )
  val pathMap2 = Seq(
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Process of Care Measures - Blood Clot Prevention and Treatment.csv" -> Seq(10),
//    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Process of Care Measures - Children.csv" ->Seq(10),
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Process of Care Measures - Heart Attack.csv" -> Seq(10),
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Process of Care Measures - Heart Failure.csv" -> Seq(10),
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Process of Care Measures - Pneumonia.csv" -> Seq(10),
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Process of Care Measures - Pregnancy and Delivery Care.csv" -> (10 to 10),
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Process of Care Measures - SCIP.csv" -> (10 to 10),
    "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Process of Care Measures - Stroke Care.csv" -> (10 to 10)
  )
  val sep = "\",\""
  val keyIdx = 0


  @Test
  def LinearRegressionTest(){


//    val mxIdx = Seq("HAI","HQQIH","Imm", "MHSPP", "MVM", "OCM", "OIEM")
    val mxIdx = Seq("BCPT","Child","HA", "HF", "Pn", "PDC", "SCIP", "SC")

    // load data create HMX
    var hmx = new HierarchyHorizontalMatrix((mxIdx zip (pathMap2.map{kv => HorizontalSparseMatrix(kv._1,sep,keyIdx, kv._2)})).toMap)
    val n = hmx.rLength
    val m = hmx.cLength
    val alpha = 0.09D
    val column = "SC"
    var theta = Seq.fill(n.toInt -1){Math.random() - 0.5D}
    println(theta.length)
    // filter noise data
    hmx = hmx.map(_.filter(d => d!=0D))
    //
    val regData = featureScale(hmx)
    val trainingData = regData.take(Seq(0,1), 3, null)
    val cvData = regData.take(Seq(2),3,null)
//    val x = hmx.rRemove("OIEM",14)
    val x = trainingData.rRemove(column,0)
    println(s"=================x======================== ${x.cLength}x ${x.rLength}" )
//    x2 printData(5)
    x.map(m => m.cMax()) printData(10)
    //    val y = hmx.rExtract("OIEM",14,15)
    val y = trainingData.rExtract(column,0,1)
    println(s"=================y========================${y.cLength}x ${y.rLength}")
    y printData(5)
    println("=================regularzation========================")
//    val regX = x.applyToChildren(mx => {(mx - mx.cAvg) / (mx.cMax - mx.cMin)})
    val regX = featureScale(x) printData(5)
//    regX.

    for (ite <- 1 to 100){
      println("===============training==========================")
      println("theta:"+ theta.mkString("[",",","]"))
      val hx = regX.dot(theta)
//      hx printData(5)
      val err = (hx - y)  / m
      println("===================error======================")
      err.cMax() printData(10)
//      (regX * err).applyToChildren(_.cMax()) printData(10)
      val grad =  (regX * err).cSum
//      println(s"gradlength:${grad.rLength} realLength:"+ grad.collect().apply("_total").length)
      val gradValue = grad.collect().apply("_total")
      val cost =  DoubleFunc.opTwoSeq(gradValue, gradValue, DoubleFunc.multiply).sum
      val delta = gradValue.map(_ * alpha)
      println("cost:" + cost)
      theta = DoubleFunc.opTwoSeq(theta, delta, DoubleFunc.minus)
    }
    val cvX = cvData.rRemove(column,0)
    val cvY = cvData.rExtract(column,0,1)
    val maxY = cvY.cMax().collect().apply("_total").apply(0)
    val minY = cvY.cMin().collect().apply("_total").apply(0)
    val cvResults = predictError(cvX,cvY,theta)
    println(s"=================cv========================${cvX.cLength}x ${cvX.rLength}")
    println("cv error:" + cvResults._1 + " relative error:" + (cvResults._1 / (maxY - minY)))

  }



  @Test
  def testHospitalEffectsLearning(){

    val targetPath = "C:/study/machinelearning/unsw-assignments/assignment-2/data/results/2014-6-5-Reg-CV-2-sqrt-error_compare-results-n-100-3.csv"
    val mxIdx = Seq("BCPT","HA", "HF", "Pn", "PDC", "SCIP", "SC")
    // load data create HMX
    var hmx = new HierarchyHorizontalMatrix((mxIdx zip (pathMap2.map{kv => HorizontalSparseMatrix(kv._1,sep,keyIdx, kv._2)})).toMap)
    val learningRate = Seq(
      5.0D,
//      0.001D,
      1.0D,
      2.0D,
      1.0D,
      1.0D,
      2.0D,
      1.0D
    )
    val numIter = 100
    val cVFolds = 2
    val src = Paths.get(targetPath)
    Files.write(src, s"num of iteration $numIter, rate:$learningRate \r\n".getBytes("UTF-8"),StandardOpenOption.APPEND,StandardOpenOption.CREATE)
    Files.write(src, s"treatment, final_cost, training_error, cross_validation_Error, theta, training_time_ms \r\n".getBytes("UTF-8"),StandardOpenOption.APPEND)
    // filter noise data
    // and do featurescale on data set
    hmx = featureScale(hmx.map(_.filter(d => d!=0D)))
    // learn co-weights for each type of disease
    for(i <- 0 until mxIdx.length){
      val x = hmx.rRemove(mxIdx(i),0)
      val y = hmx.rExtract(mxIdx(i),0,1)
      val dataSet = getCVDataSet(x,y,cVFolds)
      for(d <- dataSet) {
        val trainX = d._1
        val trainY = d._2
        val cvX = d._3
        val cvY = d._4
        val start = System.currentTimeMillis()
        val results = linearRegression(trainX, trainY,cvX, cvY, learningRate(i), numIter)
        val end = System.currentTimeMillis() - start
        //do cross-validation
        val maxY = cvY.cMax().collect().apply("_total").apply(0)
        val minY = cvY.cMin().collect().apply("_total").apply(0)
        println(s"range =${maxY - minY}")
        val cvResults = predictError(cvX, cvY, results.last._4)
        val relativeErr = cvResults._1
        //output results
        Files.write(src, s"${mxIdx(i)}, ${results.last._1}, ${results.last._2}, ${relativeErr}, ${results.last._4.mkString(",")}, $end \r\n".getBytes("UTF-8"),StandardOpenOption.APPEND)
        //output training trace data
        for(r <- results){
          val str = results.indexOf(r) + " , " + r._1 + " , " + r._2 + " , " + r._3 + " , " + r._4.mkString(" , ") + "\r\n"
          Files.write(src,str.getBytes("UTF-8"),StandardOpenOption.APPEND)
        }
      }
    }

  }

}
