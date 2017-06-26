package org.hdm.core.server

import org.hdm.core.context.HDMServerContext
import org.hdm.core.executor.ParallelTask
import org.hdm.core.model.{ParHDM, HDM}
import org.hdm.core.planing.HDMPlaner
import org.hdm.core.scheduling.Scheduler
import org.hdm.core.storage.HDMBlockManager

import scala.concurrent.{Promise, Future}

/**
 * Created by tiantian on 9/05/16.
 */
trait ServerBackend {

  val blockManager: HDMBlockManager
  val scheduler: Scheduler
  val planner: HDMPlaner
  val resourceManager: ResourceManager
  val eventManager: PromiseManager
  val dependencyManager:DependencyManager
  val hDMContext: HDMServerContext

  def init(): Unit

  def jobReceived(jobId:String, version:String, hdm:HDM[_], parallelism:Int):Future[_]


  def taskReceived[R](task:ParallelTask[R]):Future[_]

  def submitApplicationBytes(appName:String, version:String, content:Array[Byte], author:String): Unit = {
    dependencyManager.submit(appName, version, content, author, false)
  }

  def addDep(appName:String, version:String, depName:String, content:Array[Byte], author:String): Unit = {
    dependencyManager.addDep(appName, version, depName, content, author, false)
  }

  def taskSucceeded(appId:String, taskId:String, func:String, results:Seq[ParHDM[_,_]]): Unit


  def shutdown():Unit

}
