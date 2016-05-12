package org.nicta.wdy.hdm.server

import org.nicta.wdy.hdm.executor.{ParallelTask, HDMContext}
import org.nicta.wdy.hdm.model.{HDM, ParHDM}
import org.nicta.wdy.hdm.planing.{MultiClusterPlaner, HDMPlaner}
import org.nicta.wdy.hdm.scheduling.{MultiClusterScheduler}
import org.nicta.wdy.hdm.storage.HDMBlockManager

import scala.concurrent.{Promise, Future}

/**
 * Created by tiantian on 9/05/16.
 */
class MultiClusterBackend (val blockManager: HDMBlockManager,
                           override val scheduler: MultiClusterScheduler,
                           val planner: MultiClusterPlaner,
                           override val resourceManager: TreeResourceManager,
                           val eventManager: PromiseManager,
                           val dependencyManager:DependencyManager,
                           val hDMContext: HDMContext) extends ServerBackend {


  def init(): Unit = {
    scheduler.init()
    scheduler.initStateScheduling()
    new Thread{
      override def run(): Unit = {
        scheduler.startup()
      }
    }.start()
    new Thread{
      override def run(): Unit = {
        scheduler.startStateScheduling()
      }
    }.start()
  }

  def jobReceived(jobId:String, version:String, hdm:HDM[_], parallelism:Int):Future[_] ={
    val jobs = planner.planStages(hdm, parallelism)
    scheduler.addJobStages(jobs)
//    val remoteFutures = for (remoteJob <- jobs.remoteJobs) yield {
//      // create and submit a job to a remote master
//      scheduler.addRemoteJob(remoteJob)
//    }
//    val futures = for (localJob <- jobs.localJobs) yield {
//      val plans = planner.plan(localJob, parallelism)
//      dependencyManager.addPlan(exeId, plans)
//      scheduler.submitJob(jobId, version, exeId, plans.physicalPlan)
//    }
//    futures.last
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
