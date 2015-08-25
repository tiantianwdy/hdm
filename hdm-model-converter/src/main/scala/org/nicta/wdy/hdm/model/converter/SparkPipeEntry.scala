package org.nicta.wdy.hdm.model.converter

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.InputFormatInfo
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by tiantian on 13/07/15.
 */
object SparkPipeEntry {

  var pipe:SparkPipe[_, _] = _

  def main(args:Array[String]): Unit ={
    if(pipe eq null) {
      System.exit(1)
    }

    val start = System.currentTimeMillis()

    val sparkConf = new SparkConf().setAppName(pipe.name).setMaster(pipe.executionContext)
    val conf = new Configuration()
    val sc = new SparkContext(sparkConf,
      InputFormatInfo.computePreferredLocations(
        Seq(new InputFormatInfo(conf, classOf[org.apache.hadoop.mapred.TextInputFormat], pipe.inputPath))
      ))

    val data = pipe.inputFormat match {
      case "text" => sc.textFile(pipe.inputPath)
      case "json" => sc.textFile(pipe.inputPath)
      case x => sc.textFile(pipe.inputPath)
    }
    val clsTag = pipe.inType
    val output = pipe.asInstanceOf[SparkPipe[clsTag.type , _]].process(data.asInstanceOf[RDD[clsTag.type]])

    pipe.outputFormat match {
      case "text" => output.saveAsTextFile(pipe.outputPath)
      case "json" => output.saveAsTextFile(pipe.outputPath)
      case x => output.saveAsTextFile(pipe.outputPath)
    }

    sc.stop()
    // send call back to pipe.pipeServer
    pipe.notify("COMPLETED")

  }

}
