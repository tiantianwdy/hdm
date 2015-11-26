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

    val start = System.currentTimeMillis()

    val benchmark = dataTag match {
      case "userVisits" => new KVBasedPrimitiveBenchmark(context, 1, 3)
      case "ranking" => new KVBasedPrimitiveBenchmark(context)
      case x => new KVBasedPrimitiveBenchmark(context)
    }

    val iterativeBenchmark = new IterationBenchmark()
    val sqlBenchmark = new RankingSQLBenchmark()
    val uservisitsSQL = new UservisitsSQLBenchmark()
    HDMContext.init(leader = context, slots = 0)//not allow local running
    Thread.sleep(64)
    val endInit = System.currentTimeMillis()

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
      case "sort" =>
        benchmark.testTeraSort(data, len)(parallelism)
      case "mapCount" =>
        benchmark.testMapCount(data, parallelism)
      //tests for iterations
      case "iteration" =>
        iterativeBenchmark.testGeneralIteration(data, parallelism)
      case "iterativeAggregation" =>
        iterativeBenchmark.testIterationWithAggregation(data, parallelism)
      case "LR" =>
        val regressionBenchmark = new IterationBenchmark(1, 1)
        regressionBenchmark.testLinearRegression(data, 3, parallelism)
      case "weatherLR" =>
        iterativeBenchmark.testWeatherLR(data, 12, 3, parallelism, false)
      case "weatherLRCached" =>
        iterativeBenchmark.testWeatherLR(data, 12, 3, parallelism, true)
      case "weatherKMeans" =>
        iterativeBenchmark.testWeatherKMeans(data, 12, 3, parallelism, 128, false)
        //tests for SQL
      case "select" =>
        if(dataTag == "userVisits")
          uservisitsSQL.testSelect(data, parallelism, len)
        else
          sqlBenchmark.testSelect(data, parallelism, len)
      case "where" =>
        if(dataTag == "userVisits")
          uservisitsSQL.testWhere(data, parallelism, len, 0.5F)
        else
          sqlBenchmark.testWhere(data, parallelism, len, 50)
      case "orderBy" =>
        if(dataTag == "userVisits")
          uservisitsSQL.testOrderBy(data, parallelism, len)
        else
          sqlBenchmark.testOrderBy(data, parallelism, len)
      case "aggregation" =>
        if(dataTag == "userVisits")
          uservisitsSQL.testAggregation(data, parallelism, len)
        else
          sqlBenchmark.testAggregation(data, parallelism, len)
    }

    res match {
      case hdm:HDM[_,_] =>
        onEvent(hdm, "compute", start, endInit)(parallelism)
      case other:Any => //do nothing
    }


  }


  def onEvent(hdm:HDM[_,_], action:String, start:Long, endInit:Long)(implicit parallelism:Int) = action match {
    case "compute" =>
      hdm.compute(parallelism).map { hdm =>
        val end = System.currentTimeMillis()
        println(s"Job initiated in ${endInit - start} ms.")
        println(s"Job executed in ${end - endInit} ms.")
        println(s"Job completed in ${end - start} ms. And received response: ${hdm.id}")
        val size = hdm.children.map(_.blockSize).reduce(_ + _)
        println(s"results size:$size bytes with ${hdm.children.size} blocks.")
        System.exit(0)
      }
    case "sample" =>
      //      val start = System.currentTimeMillis()
      hdm.sample(25, 500000).foreach(println(_))
    case "collect" =>
      hdm.traverse.map(itr => println(itr.size))
    case x =>
  }
}
