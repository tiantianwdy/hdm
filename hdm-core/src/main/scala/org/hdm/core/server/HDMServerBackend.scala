package org.hdm.core.server

import org.hdm.core.executor.ParallelTask
import org.hdm.core.model.{HDM, ParHDM}
import org.hdm.core.planing.HDMPlaner
import org.hdm.core.scheduling.Scheduler
import org.hdm.core.server.HDMServerContext
import org.hdm.core.storage.HDMBlockManager

import scala.concurrent.{Future, Promise}

/**
 * Created by tiantian on 24/08/15.
 */
class HDMServerBackend(val blockManager: HDMBlockManager,
                       val scheduler: Scheduler,
                       val planner: HDMPlaner,
                       val resourceManager: ResourceManager,
                       val eventManager: PromiseManager,
                       val dependencyManager:DependencyManager,
                       val hDMContext: HDMServerContext) extends ServerBackend {




  def init(): Unit = {
    scheduler.init()
    new Thread{
      override def run(): Unit = {
        scheduler.startup()
      }
    }.start()
  }

  def jobReceived(jobId:String, version:String, hdm:HDM[_], parallelism:Int):Future[_] ={
    val exeId = dependencyManager.addInstance(jobId, version, hdm)
    val start = System.currentTimeMillis()
    val plans = hDMContext.explain(hdm, parallelism)
    val end = System.currentTimeMillis() - start
    dependencyManager.addPlan(exeId, plans)
    scheduler.totalScheduleTime.addAndGet(end)
    scheduler.submitJob(jobId, version, exeId, plans.physicalPlan)
  }


  def taskReceived[R](task:ParallelTask[R]):Future[_] = {
    val promise = Promise[ParHDM[_, R]]()
    eventManager.addPromise(task.taskId, promise)
    scheduler.addTask(task)
    promise.future
  }


  def taskSucceeded(appId:String, taskId:String, func:String, results:Seq[ParHDM[_,_]]): Unit ={
    scheduler.taskSucceeded(appId, taskId, func, results)
  }


  def shutdown() ={
    scheduler.stop()
  }

}



