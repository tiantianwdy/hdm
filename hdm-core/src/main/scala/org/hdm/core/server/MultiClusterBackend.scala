package org.hdm.core.server

import org.hdm.core.executor.{ParallelTask, HDMContext}
import org.hdm.core.model.{HDM, ParHDM}
import org.hdm.core.planing.{MultiClusterPlaner, HDMPlaner}
import org.hdm.core.scheduling.{MultiClusterScheduler}
import org.hdm.core.storage.HDMBlockManager

import scala.concurrent.{Promise, Future}

/**
 * Created by tiantian on 9/05/16.
 */
class MultiClusterBackend (val blockManager: HDMBlockManager,
                           override val scheduler: MultiClusterScheduler,
                           val planner: MultiClusterPlaner,
                           override val resourceManager: MultiClusterResourceManager,
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

    new Thread{
      override def run(): Unit = {
        scheduler.startRemoteTaskScheduling()
      }
    }.start()
  }

  def jobReceived(jobId:String, version:String, hdm:HDM[_], parallelism:Int):Future[_] ={
    val start = System.currentTimeMillis()
    val jobs = planner.planStages(hdm, parallelism)
    val end = System.currentTimeMillis() - start
    scheduler.totalScheduleTime.addAndGet(end)
    scheduler.addJobStages(jobId + "#" + version, jobs)
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

  def coordinationJobReceived(jobId:String, version:String, hdm:HDM[_], parallelism:Int):Future[_] ={
    val exeId = dependencyManager.addInstance(jobId, version, hdm)
    val plans = planner.plan(hdm, parallelism)
    dependencyManager.addPlan(exeId, plans)
    scheduler.submitJob(jobId, version, exeId, plans.physicalPlan)
  }


  def taskReceived[R](task:ParallelTask[R]):Future[_] = {
//    val promise = Promise[ParHDM[_, R]]()
    val promise = eventManager.createPromise[HDM[R]](task.taskId)
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
