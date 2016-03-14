package org.nicta.wdy.hdm.benchmark

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.executor.HDMContext._
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.HDM

/**
 * Created by tiantian on 26/11/15.
 */
class UservisitsSQLBenchmark extends Serializable {

  def init(context:String, localCores:Int = 0): Unit ={
    HDMContext.init(leader = context, slots = localCores)
    Thread.sleep(100)
  }

  case class Uservisits(sourceIP:String,
                         destURL:String,
                         visitData:String,
                         adRevenue:Float,
                         userAgent:String,
                         countryCode:String,
                         languageCode:String,
                         searchWorld:String,
                         duration:Int) extends Serializable

  def testSelect(dataPath:String, parallelism:Int = 4, len:Int) = {
    val path = Path(dataPath)
    val hdm = HDM(path).map(_.split(",")).map {
      line => if (len > 0)
        Uservisits(line(0), line(1).substring(0, len), line(2), line(3).toFloat, line(4), line(5), line(6), line(7), line(8).toInt)
      else
        Uservisits(line(0), line(1).substring(0, len), line(2), line(3).toFloat, line(4), line(5), line(6), line(7), line(8).toInt)
    }.map(r => (r.sourceIP, r.destURL, r.adRevenue))
    hdm
  }

  def testWhere(dataPath:String, parallelism:Int = 4, len:Int, value:Float) = {
    val path = Path(dataPath)
    val hdm = HDM(path).map(_.split(",")).map {
      line => if (len > 0)
        Uservisits(line(0), line(1).substring(0, len), line(2), line(3).toFloat, line(4), line(5), line(6), line(7), line(8).toInt)
      else
        Uservisits(line(0), line(1).substring(0, len), line(2), line(3).toFloat, line(4), line(5), line(6), line(7), line(8).toInt)
    }.filter(_.adRevenue > value)
    hdm
  }

  def testOrderBy(dataPath:String, p:Int = 4, len:Int) = {
    implicit val parallelism = p
    val path = Path(dataPath)
    val compare = (r:(String, Float), r2:(String, Float)) => {
      Ordering[Float].compare(r._2, r2._2)
    }
    val hdm = HDM(path).map(_.split(",")).map {
      line => if (len > 0)
        Uservisits(line(0), line(1).substring(0, len), line(2), line(3).toFloat, line(4), line(5), line(6), line(7), line(8).toInt)
      else
        Uservisits(line(0), line(1).substring(0, len), line(2), line(3).toFloat, line(4), line(5), line(6), line(7), line(8).toInt)
    }.map(r => (r.destURL, r.adRevenue)).sortBy(compare)
    hdm
  }

  def testAggregation(dataPath:String, parallelism:Int = 4, len:Int) = {
    val path = Path(dataPath)
    val hdm = HDM(path).map(_.split(",")).map {
      line => if (len > 0)
        Uservisits(line(0), line(1).substring(0, len), line(2), line(3).toFloat, line(4), line(5), line(6), line(7), line(8).toInt)
      else
        Uservisits(line(0), line(1).substring(0, len), line(2), line(3).toFloat, line(4), line(5), line(6), line(7), line(8).toInt)
    }.map(r => r.destURL -> r.adRevenue).reduceByKey(_ + _)
    hdm
  }

}
