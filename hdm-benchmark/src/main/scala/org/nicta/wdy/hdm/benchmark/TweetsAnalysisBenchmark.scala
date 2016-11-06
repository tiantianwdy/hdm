package org.nicta.wdy.hdm.benchmark

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.HDM

/**
 * Created by tiantian on 6/11/16.
 */
class TweetsAnalysisBenchmark (val context:String, val kIndex:Int = 0, val vIndex:Int = 1) extends  Serializable {

  import org.nicta.wdy.hdm.executor.HDMContext._

  def init(context:String, localCores:Int = 0): Unit ={
    val hDMContext = HDMContext.defaultHDMContext
    hDMContext.init(leader = context, slots = localCores)
    Thread.sleep(100)
    hDMContext
  }


  def testFindingTweets(dataPath:String, keyLen:Int = 3, parallelism:Int = 4, key:String):HDM[_] = {
    val path = Path(dataPath)
    val input = HDM(path)

    val tweets = input.map{ line =>
      val seq = line.split(",")
      new Tweets(seq)
    }

    val grouped = tweets.groupBy(_.hashTag)
    val results = grouped.findByKey(_.startsWith(key))
    results
  }


  def testAnalysisTweets(dataPath:String, keyLen:Int = 3, p:Int = 4) = {
    implicit  val parallelsm = p
    val path = Path(dataPath)
    val input = HDM(path)
    val start = System.currentTimeMillis()

    val tweets = input.map{ line =>
      val seq = line.split(",")
      new Tweets(seq)
    }.cache()

    val grouped = tweets.groupBy(_.hashTag)
    val trumpN = grouped.findByKey(_.startsWith("t")).count().collect().next()
    val hillaryN = grouped.findByKey(_.startsWith("h")).count().collect().next()

    val result = trumpN / hillaryN.toFloat
    val end = System.currentTimeMillis()
    println(s"Job completed in ${end - start} ms. \nObtained analysis result: ${result}")
  }


}

case class Tweets(author:String,
             contents:String,
             hashTag:String,
             time:String) extends Serializable {

  def this(seq:Array[String]){
    this(seq(0), seq(2), seq(1).substring(0, 3), seq(3))
  }

}
