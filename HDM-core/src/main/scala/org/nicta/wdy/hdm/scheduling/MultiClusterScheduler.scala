package org.nicta.wdy.hdm.scheduling

import akka.actor.ActorSystem
import org.nicta.wdy.hdm.executor.ParallelTask
import org.nicta.wdy.hdm.model.HDM
import org.nicta.wdy.hdm.server.{ProvenanceManager, ResourceManager, PromiseManager}
import org.nicta.wdy.hdm.storage.HDMBlockManager

import scala.concurrent.{Promise, ExecutionContext}
import scala.reflect.ClassTag

/**
 * Created by tiantian on 10/05/16.
 */
class MultiClusterScheduler(override val blockManager:HDMBlockManager,
                            override val promiseManager:PromiseManager,
                            override val resourceManager: ResourceManager,
                            override val historyManager: ProvenanceManager,
                            override val actorSys:ActorSystem,
                            override val schedulingPolicy:SchedulingPolicy)(implicit override val executorService:ExecutionContext) extends AdvancedScheduler(blockManager,
                                                                                                                                                              promiseManager,
                                                                                                                                                             resourceManager,
                                                                                                                                                             historyManager,
                                                                                                                                                             actorSys,
                                                                                                                                                             schedulingPolicy) {

  def scheduleRemoteTask[R: ClassTag](task: ParallelTask[R]): Promise[HDM[R]] = {
    val promise = promiseManager.createPromise[HDM[R]](task.taskId)
    val candidates = Scheduler.getAllAvailableWorkers(resourceManager.getAllResources())
    val workerPath = candidates.head.toString
    resourceManager.decResource(workerPath, 1)
    super.runRemoteTask(workerPath, task)
    //todo replace with planner.nextPlanning
    promise
  }

}
