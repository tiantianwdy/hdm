package org.nicta.wdy.hdm.benchmark

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.executor.HDMContext._
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.HDM

/**
 * Created by tiantian on 19/11/15.
 */
class RankingSQLBenchmark extends Serializable {

  def init(context:String, localCores:Int = 0): Unit ={
    val hDMContext = HDMContext.defaultHDMContext
    hDMContext.init(leader = context, slots = localCores)
    Thread.sleep(100)
    hDMContext
  }

  case class Ranking(url:String, rank:Float, duration:Float) extends Serializable

  def testSelect(dataPath:String, parallelism:Int = 4, len:Int) = {
    val path = Path(dataPath)
    val hdm = HDM(path).map(_.split(",")).map {
      line => if (len > 0)
        Ranking(line(0).substring(0, len), line(1).toFloat, line(2).toFloat)
      else
        Ranking(line(0), line(1).toFloat,  line(2).toFloat)
    }.map(r => r.url -> r.rank)
    hdm
  }


  def testWhere(dataPath:String, parallelism:Int = 4, len:Int, value:Int) = {
    val path = Path(dataPath)
    val hdm = HDM(path).map(_.split(",")).map {
      line => if (len > 0)
        Ranking(line(0).substring(0, len), line(1).toFloat, line(2).toFloat)
      else
        Ranking(line(0), line(1).toFloat,  line(2).toFloat)
    }.filter(_.rank > value)
    hdm
  }

  def testOrderBy(dataPath:String, p:Int = 4, len:Int) = {
    implicit val parallelism = p
    val path = Path(dataPath)
    val compare = (r:Ranking, r2:Ranking) => {
      Ordering[Float].compare(r.rank, r2.rank)
    }
    val hdm = HDM(path).map(_.split(",")).map {
      line => if (len > 0)
        Ranking(line(0).substring(0, len), line(1).toFloat, line(2).toFloat)
      else
        Ranking(line(0), line(1).toFloat,  line(2).toFloat)
    }.sortBy(compare)
    hdm
  }

  def testAggregation(dataPath:String, parallelism:Int = 4, len:Int) = {
    val path = Path(dataPath)
    val hdm = HDM(path).map(_.split(",")).map {
      line => if (len > 0)
        Ranking(line(0).substring(0, len), line(1).toFloat, line(2).toFloat)
      else
        Ranking(line(0), line(1).toFloat,  line(2).toFloat)
    }.map(r => r.url -> r.rank).reduceByKey(_ + _)
    hdm
  }

}
