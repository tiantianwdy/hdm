package org.nicta.unsw.ml

import org.nicta.unsw.ml.HorizontalSparseMatrix
import java.nio.file.{StandardOpenOption, Files, Paths}
import org.nicta.unsw.ml.LinearRegressionFunc._

/**
 * Created by Dongyao.Wu on 2014/6/5.
 */
object HospitalCareMining {

  val fileMap = Seq(
    "Process of Care Measures - Blood Clot Prevention and Treatment.csv" -> Seq(10),
    "Process of Care Measures - Heart Attack.csv" -> Seq(10),
    "Process of Care Measures - Heart Failure.csv" -> Seq(10),
    "Process of Care Measures - Pneumonia.csv" -> Seq(10),
    "Process of Care Measures - Pregnancy and Delivery Care.csv" -> (10 to 10),
    "Process of Care Measures - SCIP.csv" -> (10 to 10),
    "Process of Care Measures - Stroke Care.csv" -> (10 to 10)
  )
  
  val mxIdx = Seq("BCPT","HA", "HF", "Pn", "PDC", "SCIP", "SC")
  
  val sep = "\",\""
  val keyIdx = 0
  val defaultPath = "c:/study/machinelearning/unsw-assignments/assignment-2/data/hospital_data/"
  val defaultResultPath = "c:/study/machinelearning/unsw-assignments/assignment-2/results/hospital_care_mining-reg-CV-2-sqrt--results-n-100.csv"

   def main (args: Array[String]) {
     val dataBase = if(args.length > 0) args(0) else defaultPath
     val targetPath = if(args.length > 1) args(1) else defaultResultPath
     val numIter = if(args.length > 2) args(2).toInt else 10
     val cVFolds = if(args.length > 3) args(3).toInt else 2
     val pathMap = fileMap.map(kv => (dataBase + kv._1) -> kv._2)
     val learningRate = Seq(
       5.0D,
       1.0D,
       2.0D,
       1.0D,
       1.0D,
       2.0D,
       1.0D
     )

     val src = Paths.get(targetPath)
     Files.write(src, s"num of iteration $numIter, rate:$learningRate \r\n".getBytes("UTF-8"),StandardOpenOption.APPEND,StandardOpenOption.CREATE)
     Files.write(src, s"treatment, final_cost, training_error, cross_validation_Error, theta, training_time_ms \r\n".getBytes("UTF-8"),StandardOpenOption.APPEND)
     // load data create HMX
     var hmx = new HierarchyHorizontalMatrix((mxIdx zip (pathMap.map{kv => HorizontalSparseMatrix(kv._1,sep,keyIdx, kv._2)})).toMap)
     // and do feature scaling on data set and filter noise data
     hmx = featureScale(hmx.map(_.filter(d => d!=0D)))
     // learn co-weights for each type of disease
     for(i <- 0 until mxIdx.length){
       // remove target attribute from data set
       val x = hmx.rRemove(mxIdx(i),0)
       val y = hmx.rExtract(mxIdx(i),0,1)
       // split data set for cross-validation
       val dataSet = getCVDataSet(x,y,cVFolds)
       for(d <- dataSet) {
         val trainX = d._1
         val trainY = d._2
         val cvX = d._3
         val cvY = d._4
         val start = System.currentTimeMillis()
         val results = linearRegression(trainX, trainY,cvX, cvY, learningRate(i), numIter)
         val end = System.currentTimeMillis() - start
         val minY = cvY.cMin().collect().apply("_total").apply(0)
         val maxY = cvY.cMax().collect().apply("_total").apply(0)
         //do cross-validation
         val cvResults = predictError(cvX, cvY, results.last._4)
         val relativeErr = cvResults._1
         //output results
         Files.write(src, s"${mxIdx(i)}, ${results.last._1}, ${results.last._2}, ${relativeErr}, ${results.last._4.mkString(",")}, $end \r\n".getBytes("UTF-8"),StandardOpenOption.APPEND)
         //output data about training process
         for(r <- results){
           val str = results.indexOf(r) + " , " + r._1 + " , " + r._2 + " , " + r._3 + " , " + r._4.mkString(" , ") + "\r\n"
           Files.write(src,str.getBytes("UTF-8"),StandardOpenOption.APPEND)
         }
       }
     }
  }

}
