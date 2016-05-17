package org.nicta.wdy.hdm.scheduling

import java.util.concurrent.{Semaphore, ConcurrentHashMap, LinkedBlockingQueue}

import akka.actor.ActorSystem
import org.nicta.wdy.hdm.executor.{ClusterExecutor, HDMContext, Partitioner, ParallelTask}
import org.nicta.wdy.hdm.functions.ParallelFunction
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.{DDM, ParHDM, DFM, HDM}
import org.nicta.wdy.hdm.planing.{MultiClusterPlaner, JobStage}
import org.nicta.wdy.hdm.server._
import org.nicta.wdy.hdm.server.provenance.ExecutionTrace
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Lock, Future, Promise, ExecutionContext}
import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

/**
 * Created by tiantian on 10/05/16.
 */
class MultiClusterScheduler(override val blockManager:HDMBlockManager,
                            override val promiseManager:PromiseManager,
                            override val resourceManager: MultiClusterResourceManager,
                            override val historyManager: ProvenanceManager,
                            override val actorSys:ActorSystem,
                            val dependencyManager:DependencyManager,
                            val planner: MultiClusterPlaner,
                            override val schedulingPolicy:SchedulingPolicy)
                           (implicit override val executorService:ExecutionContext)
                            extends AdvancedScheduler(blockManager,
                                                      promiseManager,
                                                      resourceManager,
                                                      historyManager,
                                                      actorSys,
                                                      schedulingPolicy) {

  private val stateQueue = new LinkedBlockingQueue[JobStage]()

  private val remoteTaskQueue = new LinkedBlockingQueue[ParallelTask[_]]()
  private val localTaskQueue = new LinkedBlockingQueue[ParallelTask[_]]()

  protected val remoteTaskNonEmptyLock = new Semaphore(0)
  protected val localTaskNonEmptyLock = new Semaphore(0)

  private val appStateBuffer: java.util.Map[String, ListBuffer[JobStage]] = new ConcurrentHashMap[String, ListBuffer[JobStage]]()




  def initStateScheduling(): Unit ={
    stateQueue.clear()
    appStateBuffer.clear()
    remoteTaskQueue.clear()
  }


  def startLocalTaskScheduling(): Unit = {
    isRunning.set(true)
    while (isRunning.get) try {
      if(localTaskQueue.isEmpty) {
        nonEmptyLock.acquire()
      }
      resAccessorlock.acquire()
      resourceManager.waitForChildrenNonEmpty()
      scheduleOnResource(localTaskQueue, resourceManager.getChildrenRes())
      resAccessorlock.release()
    }
  }

  def startRemoteTaskScheduling(): Unit ={
    while (isRunning.get) {
//      if(remoteTaskQueue.isEmpty) {
//        remoteTaskNonEmptyLock.acquire()
//      }
      val task = remoteTaskQueue.take()
      resAccessorlock.acquire()
      log.info(s"Waiting for resources...")
      resourceManager.waitForChildrenNonEmpty()
      log.info(s"Completed waiting for resources...")
      // todo use scheduling policy to choose optimal candidates
      //      scheduleOnResource(remoteTaskQueue, resourceManager.getChildrenRes())
      val candidates = Scheduler.getAllAvailableWorkers(resourceManager.getChildrenRes())
      val workerPath = candidates.head.toString
      resourceManager.decResource(workerPath, 1)
      resAccessorlock.release()
      log.info(s"A remote task has been assigned to: [$workerPath] [${task.taskId + "_" + task.func.toString}}] ")
      if (Path.isLocal(workerPath)) ClusterExecutor.runTask(task)
      else runRemoteTask(workerPath, task)
    }
  }

  def scheduleLocalTask[R: ClassTag](task: ParallelTask[R]): Promise[HDM[R]] = {
    // todo change to using a scheduling thread and scheduling on local nodes
    val promise = promiseManager.createPromise[HDM[R]](task.taskId)
    localTaskQueue.offer(task)
    localTaskNonEmptyLock.release()
    promise
  }

  def scheduleRemoteTask[R: ClassTag](task: ParallelTask[R]): Promise[HDM[R]] = {
    // todo change to using a scheduling thread and scheduling on local nodes
    val promise = promiseManager.createPromise[HDM[R]](task.taskId)
    remoteTaskQueue.offer(task)
    remoteTaskNonEmptyLock.release()
    promise
  }


  def runRemoteJob(hdm:HDM[_], parallelism:Int):Future[HDM[_]] = {
    HDMContext.defaultHDMContext.compute(hdm, parallelism)
  }

  def addJobStages(states:Seq[JobStage]): Future[HDM[_]] = {
    states.map{ sg =>
      val promise = promiseManager.createPromise[HDM[_]](sg.jobId)
      if(sg.parents == null || sg.parents.isEmpty){
        stateQueue.offer(sg)
      } else {
        appStateBuffer.getOrElseUpdate(sg.appId, new ListBuffer[JobStage]()) += sg
      }
      promise.future
    }.last
  }

  def startStateScheduling(): Unit ={
    while(isRunning.get()){
      val stage = stateQueue.take()
      val hdm = stage.job
      val appName = hdm.appContext.appName
      val version = hdm.appContext.version
      val exeId = dependencyManager.addInstance(appName, version , hdm)
      if(stage.isLocal){
        //if job is local
        val plans = planner.plan(hdm, stage.parallelism)
        dependencyManager.addPlan(exeId, plans)
        submitJob(appName, version, exeId, plans.physicalPlan)
      } else {
        // send to remote master state based on context
        val ref = runRemoteJob(hdm, stage.parallelism)
        blockManager.addRef(hdm)
        ref
      }.onComplete {
        case Success(resHDM) => synchronized {
          log.info(s"A Job succeed with ${resHDM}")
          jobSucceed(appName+"#"+version, hdm.id, resHDM.blocks)
        }
        case Failure(f) =>
      }
    }
  }

  def jobSucceed(appId:String, jobId:String, blks:Seq[String]): Unit = {
    // update hdm status
    val ref = blockManager.getRef(jobId) match {
      case dfm: DFM[_, _] =>
        val children = dfm.children.asInstanceOf[Seq[ParHDM[_, dfm.inType.type]]]
        //        dfm.copy(blocks = blks, state = Computed)
        DFM(children,
          jobId,
          dfm.dependency,
          dfm.func.asInstanceOf[ParallelFunction[dfm.inType.type, dfm.outType.type]],
          blks,
          dfm.distribution,
          dfm.location,
          dfm.preferLocation,
          dfm.blockSize, dfm.isCache, Computed,
          dfm.parallelism, dfm.keepPartition,
          dfm.partitioner.asInstanceOf[Partitioner[dfm.outType.type]],
          dfm.appContext)
      case ddm: DDM[_, _] => ddm.copy(state = Computed)
    }
    blockManager.addRef(ref)
    // trigger following jobs
    triggerStage(appId)

  }

  def triggerStage(appId:String): Unit = {
    val stages  = appStateBuffer.get(appId)
    log.info(s"finding stages: ${stages}")
    if(stages != null && stages.nonEmpty){
      val nextStages = stages.filter { sg =>
        sg.parents.forall { job =>
          val ref = blockManager.getRef(job.jobId)
          ref.state == Computed
        }
      }
      log.info(s"New stages are triggered: ${nextStages}")
      nextStages.foreach(stateQueue.offer(_))
    }
  }



}
