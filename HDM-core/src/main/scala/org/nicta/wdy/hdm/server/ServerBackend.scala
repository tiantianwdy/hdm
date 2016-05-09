package org.nicta.wdy.hdm.server

import org.nicta.wdy.hdm.executor.{HDMContext, ParallelTask}
import org.nicta.wdy.hdm.model.{ParHDM, HDM}
import org.nicta.wdy.hdm.planing.HDMPlaner
import org.nicta.wdy.hdm.scheduling.Scheduler
import org.nicta.wdy.hdm.storage.HDMBlockManager

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
  val hDMContext: HDMContext

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
