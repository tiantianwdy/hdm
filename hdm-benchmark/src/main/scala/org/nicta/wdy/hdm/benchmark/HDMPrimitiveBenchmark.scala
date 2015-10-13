package org.nicta.wdy.hdm.benchmark

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.HDM

import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

/**
 * Created by tiantian on 7/01/15.
 */

object HDMBenchmark {


  def main(args:Array[String]): Unit ={
    println("Cmd format: [masterPath] [dataPath] [testTag] [parallelism] [param]")
    println(s"params length:${args.length}" + args.mkString)
    val context = args(0)
    val data = args(1)
    val testTag = args(2)
    val parallelism = args(3).toInt
    val dataTag = if(args.length >= 5) args(4) else "ranking"
    val len = if(args.length >= 6) args(5).toInt else 3
    val benchmark = dataTag match {
      case "userVisits" => new KVBasedPrimitiveBenchmark(context, 1, 3)
      case "ranking" => new KVBasedPrimitiveBenchmark(context)
      case x => new KVBasedPrimitiveBenchmark(context)
    }
    val iterativeBenchmark = new IterationBenchmark()
    HDMContext.init(leader = context, slots = 0)//not allow local running
    Thread.sleep(100)

    val res = testTag match {
      case "map" =>
        benchmark.testMap(data, len, parallelism)
      case "multiMap" =>
        benchmark.testMultipleMap(data, len, parallelism)
      case "multiMapFilter" =>
        benchmark.testMultiMapFilter(data, len, parallelism, "a")
      case "groupBy" =>
        benchmark.testGroupBy(data, len, parallelism)
      case "reduceByKey" =>
        benchmark.testReduceByKey(data, len, parallelism)
      case "groupReduce" =>
        benchmark.testGroupMapValues(data, len, parallelism)
      case "findByKey" =>
        benchmark.testFindByKey(data, len, parallelism, "a")
      case "top" =>
        benchmark.testTop(data, len, parallelism)
      case "mapCount" =>
        benchmark.testMapCount(data, parallelism)
      case "iteration" =>
        iterativeBenchmark.testGeneralIteration(data, parallelism)
      case "iterativeAggregation" =>
        iterativeBenchmark.testIterationWithAggregation(data, parallelism)

    }
    res match {
      case hdm:HDM[_,_] =>
        onEvent(hdm, "compute")(parallelism)
      case other:Any => //do nothing
    }


  }


  def onEvent(hdm:HDM[_,_], action:String)(implicit parallelism:Int) = action match {
    case "compute" =>
      val start = System.currentTimeMillis()
      hdm.compute(parallelism).map { hdm =>
        println(s"Job completed in ${System.currentTimeMillis() - start} ms. And received response: ${hdm.id}")
        hdm.children.foreach(println(_))
        System.exit(0)
      }
    case "sample" =>
      //      val start = System.currentTimeMillis()
      hdm.sample(25).map(iter => iter.foreach(println(_)))
    case "collect" =>
      hdm.traverse.map(itr => println(itr.size))
    case x =>
  }
}
