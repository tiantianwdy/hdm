package org.nicta.wdy.hdm.benchmark

import breeze.linalg.DenseVector
import org.nicta.wdy.hdm.executor.{AppContext, HDMContext}
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.HDM
import scala.concurrent.ExecutionContext.Implicits.global


/**
 * Created by tiantian on 7/05/16.
 */
class MultiClusterBenchmark(val master1:String, val master2:String) extends Serializable{

  val hDMContext = HDMContext.defaultHDMContext

  val appContext1 = new AppContext(appName = "hdm-examples", version = "0.0.1", masterPath = master1)

  val appContext2 = new AppContext(appName = "hdm-examples", version = "0.0.1", masterPath = master2)


  def testParallelExecution(dataPath1:String, dataPath2:String)(implicit parallelism:Int): Unit ={
    import HDMContext._
    hDMContext.init(leader = master1, 0)
    Thread.sleep(200)

    val vecLen = 10
    val data1 = Path(dataPath1)
    val data2 = Path(dataPath2)
    val dataDP1 = HDM(data1, appContext1)
    val dataDP2 = HDM(data2, appContext2)

    val trainingDp1 = dataDP1.map(line => line.split("\\s+"))
      .map{ seq => seq.drop(3).dropRight(6)}
      .filter(seq => seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{ seq => seq.take(vecLen).map(_.toDouble)}
      .zipWithIndex.mapValues(d => DenseVector(d)).cache()

    val trainingDp2 = dataDP2.map(line => line.split("\\s+"))
      .map{ seq => seq.drop(3).dropRight(6)}
      .filter(seq => seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{ seq => seq.take(vecLen).map(_.toDouble)}
      .zipWithIndex.mapValues(d => DenseVector(d)).cache()

    println(trainingDp1.count().collect().next())
    println(trainingDp2.count().collect().next())


  }

  def testShuffleTask(dataPath1:String, dataPath2:String)(implicit parallelism:Int): Unit ={
    import HDMContext._
    hDMContext.init(leader = master1, 0)

    val vecLen = 10
    val data1 = Path(dataPath1)
    val data2 = Path(dataPath2)
    val dataDP1 = HDM(data1, appContext1)
    val dataDP2 = HDM(data2, appContext2)

    val trainingDp1 = dataDP1.map(line => line.split("\\s+"))
      .map{ seq => seq.drop(3).dropRight(6)}
      .filter(seq => seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{ seq => seq.take(vecLen).map(_.toDouble)}.map(arr => arr(0) -> arr)
    //      .zipWithIndex.mapValues(d => DenseVector(d))

    val trainingDp2 = dataDP2.map(line => line.split("\\s+"))
      .map{ seq => seq.drop(3).dropRight(6)}
      .filter(seq => seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{ seq => seq.take(vecLen).map(_.toDouble)}.map(arr => arr(0) -> arr)
    //      .zipWithIndex.mapValues(d => DenseVector(d))

    val job = trainingDp1.joinByKey(trainingDp2)

    val start = System.currentTimeMillis()

    job.compute(parallelism).map { hdm =>
      println(s"Job completed in ${System.currentTimeMillis() - start} ms. And received response: ${hdm.id}")
      hdm.blocks.foreach(println(_))
      System.exit(0)
    }
  }


  def onEvent(hdm:HDM[_], action:String)(implicit parallelism:Int) = action match {
    case "compute" =>
      val start = System.currentTimeMillis()
      hdm.compute(parallelism).map { hdm =>
        println(s"Job completed in ${System.currentTimeMillis() - start} ms. And received response: ${hdm.id}")
        hdm.blocks.foreach(println(_))
        System.exit(0)
      }
    case "sample" =>
      //      val start = System.currentTimeMillis()
      hdm.sample(100, 500000)foreach(println(_))
    case "collect" =>
      val start = System.currentTimeMillis()
      val itr = hdm.collect()
      println(s"Job completed in ${System.currentTimeMillis() - start} ms. And received results: ${itr.size}")
    case x =>
  }
}
