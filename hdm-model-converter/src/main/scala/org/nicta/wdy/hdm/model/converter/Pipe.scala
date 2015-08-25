package org.nicta.wdy.hdm.model.converter

import org.apache.hadoop.mapreduce.{Reducer, Mapper}
import org.apache.spark.rdd.RDD

import scala.concurrent.ExecutionContext
import scala.reflect.{ClassTag,classTag}

/**
 * Created by tiantian on 13/07/15.
 */
abstract class Pipe[I: ClassTag, O: ClassTag](val name: String,
                                              val pipelineServer: String,
                                              val executionContext: String,
                                              val inputPath: String,
                                              val inputFormat: String,
                                              val outputPath: String,
                                              val outputFormat: String) extends Serializable {

  private[hdm] def notify(status:String)
}

class SparkPipe[I:ClassTag, O:ClassTag]( name: String,
                                                  pipelineServer: String,
                                                  executionContext: String,
                                                  inputPath: String,
                                                  inputFormat: String,
                                                  outputPath: String,
                                                  outputFormat: String,
                                                  val process :(RDD[I]) => RDD[O]
                                                  ) extends Pipe[I,O](name, pipelineServer, executionContext, inputPath, inputFormat ,outputPath, outputFormat) {


  val inType = classTag[I]

  val outType = classTag[O]




  override private[hdm] def notify(status: String): Unit = ???
}

class MRPipe[KIN, VIN, KOUT, VOUT] ( name: String,
                                              pipelineServer: String,
                                              executionContext: String,
                                              val mapper:Mapper[KIN, VIN, KOUT, VOUT],
                                              val combiner:Reducer[KOUT, VOUT, KOUT, VOUT],
                                              val reducer:Reducer[KOUT, VOUT, KOUT, VOUT],
                                              inputPath: String,
                                              inputFormat: String,
                                              outputPath: String,
                                              outputFormat: String) extends Pipe[(KIN,VIN),(KOUT,VOUT)](name, pipelineServer, executionContext, inputPath, inputFormat ,outputPath, outputFormat) {
  
  override private[hdm] def notify(status: String): Unit = ???
}

