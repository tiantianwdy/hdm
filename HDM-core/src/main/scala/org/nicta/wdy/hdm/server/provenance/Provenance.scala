package org.nicta.wdy.hdm.server.provenance

import org.nicta.wdy.hdm.model.HDM

/**
 * Created by tiantian on 3/03/16.
 */

trait Provenance extends Serializable

case class ApplicationTrace (name:String,
                             version:String,
                             author:String,
                             createTime:Long,
                             dependencies:Seq[String]) extends Provenance {
  def id = s"$name#$version"

}


case class ExecutionTrace(taskId:String,
                          appName:String,
                          version:String,
                          instanceID:String,
                          function:String,
                          inputPath:Seq[String],
                          outputPath:Seq[String],
                          location:String,
                          startTime:Long,
                          endTime:Long,
                          status:String) extends Provenance {

  def id = s"$appName#$version#$instanceID#$taskId"

}


case class ExecutionDAG(nodes:Seq[ExecutionTrace],
                        links:Seq[(String, String)]) extends Provenance