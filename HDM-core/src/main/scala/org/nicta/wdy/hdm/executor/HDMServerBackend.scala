package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.model.{AbstractHDM, HDM}
import org.nicta.wdy.hdm.planing.HDMPlaner
import org.nicta.wdy.hdm.scheduling.Scheduler
import org.nicta.wdy.hdm.server.provenance.ApplicationTrace
import org.nicta.wdy.hdm.server.{DependencyManager, ProvenanceManager}
import org.nicta.wdy.hdm.storage.HDMBlockManager

import scala.concurrent.{Future, Promise}

/**
 * Created by tiantian on 24/08/15.
 */
class HDMServerBackend(val appManager: AppManager,
                       val blockManager: HDMBlockManager,
                       val scheduler: Scheduler,
                       val planner: HDMPlaner,
                       val resourceManager: ResourceManager,
                       val eventManager: PromiseManager,
                       val dependencyManager:DependencyManager) {




  def init(): Unit = {
    scheduler.init()
    new Thread{
      override def run(): Unit = {
        scheduler.startup()
      }
    }.start()
  }

  def jobReceived(jobId:String, version:String, hdm:AbstractHDM[_], parallelism:Int):Future[_] ={
    appManager.addApp(jobId, hdm)
    val plan = HDMContext.explain(hdm, parallelism)
    appManager.addPlan(jobId, plan)
    scheduler.submitJob(jobId, version, plan)
  }


  def taskReceived[R](task:ParallelTask[R]):Future[_] = {
    val promise = Promise[HDM[_, R]]()
    eventManager.addPromise(task.taskId, promise)
    scheduler.addTask(task)
    promise.future
  }

  def submitApplicationBytes(appName:String, version:String, content:Array[Byte], author:String): Unit = {
    dependencyManager.submit(appName,version, content, author, false)
  }

  def addDep(appName:String, version:String, depName:String, content:Array[Byte], author:String): Unit = {
    dependencyManager.addDep(appName, version, depName, content, author, false)
  }



  def taskSucceeded(appId:String, taskId:String, func:String, results:Seq[HDM[_,_]]): Unit ={
    scheduler.taskSucceeded(appId, taskId, func, results)
  }


  def shutdown() ={
    scheduler.stop()
  }

}



