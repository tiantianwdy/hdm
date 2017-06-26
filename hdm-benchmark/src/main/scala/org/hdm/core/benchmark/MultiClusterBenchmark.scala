package org.hdm.core.benchmark

import breeze.linalg.{Vector, DenseVector}
import org.hdm.core.context._
import org.hdm.core.io.Path
import org.hdm.core.model.HDM
import HDMContext._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.math._
import scala.util.Random


/**
 * Created by tiantian on 7/05/16.
 */
class MultiClusterBenchmark(val master1:String, val master2:String) extends Serializable {

  val hDMContext = HDMAppContext.defaultContext

  val appContext1 = new AppContext(appName = "hdm-examples", version = "0.0.1", masterPath = master1)

  val appContext2 = new AppContext(appName = "hdm-examples", version = "0.0.1", masterPath = master2)

  implicit val hDMEntry = new HDMSession(hDMContext)


  def testParallelExecution(dataPath1:String, dataPath2:String)(implicit parallelism:Int): Unit ={
    hDMEntry.init(leader = master1, 0)
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
    hDMEntry.init(leader = master1, 0)
    Thread.sleep(200)

    val vecLen = 2
    val data1 = Path(dataPath1)
    val data2 = Path(dataPath2)
    val dataDP1 = HDM(data1, appContext1)
    val dataDP2 = HDM(data2, appContext2)

    val trainingDp1 = dataDP1.map(line => line.split("\\s+"))
//      .map{ seq => seq.drop(3).dropRight(6)}
//      .filter(seq => seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{ seq => seq.take(vecLen)}.map(arr => arr(0) -> arr(1))
    //      .zipWithIndex.mapValues(d => DenseVector(d))

    val trainingDp2 = dataDP2.map(line => line.split("\\s+"))
//      .map{ seq => seq.drop(3).dropRight(6)}
//      .filter(seq => seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{ seq => seq.take(vecLen)}.map(arr => arr(0) -> arr(1))
    //      .zipWithIndex.mapValues(d => DenseVector(d))

    val job = trainingDp1.joinByKey(trainingDp2)

    val start = System.currentTimeMillis()

    job.compute(parallelism, hDMEntry).map { hdm =>
      println(s"Job completed in ${System.currentTimeMillis() - start} ms. And received response: ${hdm.id}")
      hdm.blocks.foreach(println(_))
      System.exit(0)
    }
  }


  def testMultiPartyLR(dataPath1:String, dataPath2:String, vectorLen: Int, iteration:Int)(implicit parallelism:Int) = {
    hDMEntry.init(leader = master1, 0)
    Thread.sleep(200)

    val vecLen = vectorLen / 2
    var weights = DenseVector.fill(vectorLen){0.01 * Random.nextDouble()}
    val data1 = Path(dataPath1)
    val data2 = Path(dataPath2)
    val dataDP1 = HDM(data1, appContext1)
    val dataDP2 = HDM(data2, appContext2)

    val trainingDp1 = dataDP1.map(line => line.split("\\s+"))
      .map{ seq => seq.drop(3).dropRight(6)}
      .filter(seq => seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{seq => seq.take(vecLen).map(_.toDouble)}
      .zipWithIndex.mapValues(d => DenseVector(d))


    val trainingDp2 = dataDP2.map(line => line.split("\\s+"))
      .map{ seq => seq.drop(3).dropRight(6)}
      .filter(seq => seq.forall(s => s.matches("\\d+(.\\d+)?")))
      .map{seq => seq.takeRight(vecLen).map(_.toDouble)}
      .zipWithIndex.mapValues(d => DenseVector(d))

    val training = trainingDp1.joinByKey(trainingDp2)
      .mapValues(tup => tup._1 += tup._2)
      .map{ tup =>
        val vec = Vector(tup._2.data)
        DataPoint(vec, tup._2.data(0))
      }.cache()

    var start = System.currentTimeMillis()

    for(i <- 1 to iteration) {
      val w = weights
      val gradient = training.map{ p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _).collect().next()
      weights -= gradient
      val end = System.currentTimeMillis()
      println(s"Time consumed for iteration $i : ${end - start} ms. weights: $weights")
      start = System.currentTimeMillis()
    }
    weights
  }

  def onEvent(hdm:HDM[_], action:String)(implicit parallelism:Int, hDMEntry: HDMEntry) = action match {
    case "compute" =>
      val start = System.currentTimeMillis()
      hdm.compute(parallelism, hDMEntry).map { hdm =>
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

object MultiClusterBenchmarkMain {

  def main(args:Array[String]): Unit ={
    println("Cmd format: [masterPath1] [masterPath2] [dataPath] [testTag] [parallelism] [param]")
    println(s"params length:${args.length}" + args.mkString)
    val context1 = args(0)
    val context2 = args(1)
    val data = args(2)
    val testTag = args(3)
    implicit val parallelism = args(4).toInt
    val dataTag = if(args.length >= 6) args(5) else "ranking"
    val len = if(args.length >= 7) args(6).toInt else 3

//    AppContext.defaultAppContext.appName = "hdm-examples"
//    AppContext.defaultAppContext.version = "0.0.1"
//    AppContext.defaultAppContext.masterPath = context1
//    val hDMContext = HDMContext.defaultHDMContext
//    hDMContext.init(leader = context1, slots = 0)
//    Thread.sleep(64)

    val multiclusterBenchmark = new MultiClusterBenchmark(context1, context2)

    val res = testTag match {
      case "map" =>
        multiclusterBenchmark.testParallelExecution(data, data)
      case "mc-lr" =>
        multiclusterBenchmark.testMultiPartyLR(data, data, 12, 3)
      case "lr" =>
        multiclusterBenchmark.testShuffleTask(data, data)
    }
  }

}
