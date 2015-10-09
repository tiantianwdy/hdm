package org.nicta.wdy.hdm.benchmark

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.HDM

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
    val hdm = HDM(path)
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
    val hdm = HDM(path)
    //    val kOffset = 0
    var aggregation = 0F
    for(i <- 1 to 3){
      val start = System.currentTimeMillis()
      val vOffset = 1 // only avaliable in this scope , todo: support external variables
      val agg = aggregation
      val res = hdm.map{ w =>
        val as = w.split(",")
        as(vOffset).toFloat + i*agg
      }.collect()(parallelism).take(20).toSeq
      res.foreach(println(_))
      println(res.sum)
      aggregation += res.sum
      val end = System.currentTimeMillis()
      println(s"Time consumed for iteration $i : ${end - start} ms. aggregation: $aggregation")
      Thread.sleep(100)
    }
  }


  def testIterationWithCache(dataPath:String, parallelism:Int = 4)={

    val path = Path(dataPath)
    val hdm = HDM(path).cache()
    var aggregation = 0F

    for(i <- 1 to 3) {
      val start = System.currentTimeMillis()
      val vOffset = 1 // only avaliable in this scope, todo: support external variables
      val agg = aggregation
      val res = hdm.map{ w =>
        val as = w.split(",")
        as(vOffset).toFloat + i*agg
      }.collect()(parallelism).take(20).toSeq
      aggregation += res.sum
      val end = System.currentTimeMillis()
      println(s"Time consumed for iteration $i : ${end - start} ms. aggregation: $aggregation")
      Thread.sleep(100)
    }

  }

}
