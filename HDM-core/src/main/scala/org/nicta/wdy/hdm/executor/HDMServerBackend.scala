package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.model.HDM
import org.nicta.wdy.hdm.planing.HDMPlaner
import org.nicta.wdy.hdm.scheduling.Scheduler
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
                       val eventManager: PromiseManager) {




  def init(): Unit = {
    scheduler.init()
    new Thread{
      override def run(): Unit = {
        scheduler.startup()
      }
    }.start()
  }

  def jobReceived(jobId:String, hdm:HDM[_,_], parallelism:Int):Future[_] ={
    appManager.addApp(jobId, hdm)
    val plan = HDMContext.explain(hdm, parallelism)
    appManager.addPlan(jobId, plan)
    scheduler.submitJob(jobId, plan)
  }


  def taskReceived[I, R](task:Task[_,_]):Future[_] = {
    val promise = Promise[HDM[I, R]]()
    eventManager.addPromise(task.taskId, promise)
    scheduler.addTask(task)
    promise.future
  }



  def taskSucceeded(appId:String, taskId:String, func:String, results:Seq[HDM[_,_]]): Unit ={
    scheduler.taskSucceeded(appId,taskId, func, results.map(_.toURL))
  }


  def shutdown() ={
    scheduler.stop()
  }

}



