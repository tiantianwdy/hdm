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
    val len = if(args.length >= 5) args(4).toInt else 3
    val benchmark = new HDMPrimitiveBenchmark(context)
    HDMContext.init(leader = context, slots = 0)//not allow local running
    Thread.sleep(100)

    testTag match {
      case "map" =>
        benchmark.testMap(data, len, parallelism)
      case "groupBy" =>
        benchmark.testGroupBy(data, len, parallelism)
      case "reduceByKey" =>
        benchmark.testReduceByKey(data, len, parallelism)
      case "groupReduce" =>
        benchmark.testGroupByReduce(data, len, parallelism)
      case "top" =>
        benchmark.testTop(data, len, parallelism)
      case "mapCount" =>
        benchmark.testMapCount(data, parallelism)
      case "select" =>
        benchmark.testMapSelect()
      case x =>
    }
  }

}
