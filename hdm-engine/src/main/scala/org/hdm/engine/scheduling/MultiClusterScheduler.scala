package org.hdm.core.scheduling

import java.util.concurrent._

import akka.actor.ActorSystem
import org.hdm.core.context.{HDMEntry, HDMServerContext}
import org.hdm.core.executor.{ClusterExecutor, ParallelTask}
import org.hdm.core.functions.{Partitioner, ParallelFunction}
import org.hdm.core.io.Path
import org.hdm.core.model.{DDM, ParHDM, DFM, HDM}
import org.hdm.core.planing.{MultiClusterPlaner, JobStage}
import org.hdm.core.server._
import org.hdm.core.server.provenance.ExecutionTrace
import org.hdm.core.storage.{Computed, HDMBlockManager}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Lock, Future, Promise, ExecutionContext}
import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.util.{Failure, Success}
import scala.collection.JavaConversions._

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
                            override val schedulingPolicy:SchedulingPolicy,
                            val hDMContext: HDMEntry)
                           (implicit override val executorService:ExecutionContext)
                            extends AdvancedScheduler(blockManager,
                                                      promiseManager,
                                                      resourceManager,
                                                      historyManager,
                                                      actorSys,
                                                      schedulingPolicy) {

  private val stageQueue = new LinkedBlockingQueue[JobStage]()

  private val remoteTaskQueue = new LinkedBlockingQueue[ParallelTask[_]]()
  private val localTaskQueue = new LinkedBlockingQueue[ParallelTask[_]]()

  protected val remoteTaskNonEmptyLock = new Semaphore(0)
  protected val localTaskNonEmptyLock = new Semaphore(0)

  private val appStateBuffer: java.util.Map[String, CopyOnWriteArrayList[JobStage]] = new ConcurrentHashMap[String, CopyOnWriteArrayList[JobStage]]()

  protected override def scheduleOnResource(blockingQue:BlockingQueue[ParallelTask[_]], candidatesWithIdx:Seq[(Path, Int)]): Unit = {

    val tasks= mutable.Buffer.empty[SchedulingTask]
    val candidates = candidatesWithIdx.map(_._1)
    val coreIdxMap = mutable.HashMap.empty[Path, Int] ++= candidatesWithIdx

    blockingQue.foreach{ task =>
      val ids = task.input.map(_.id)
      val inputLocations = new ArrayBuffer[Path](task.input.length)
      val inputSize = new ArrayBuffer[Long](task.input.length)
      inputLocations ++= HDMBlockManager().getLocations(ids)
      inputSize ++= HDMBlockManager().getblockSizes(ids).map(_ / 1024)
      tasks += SchedulingTask(task.taskId, inputLocations, inputSize, task.dep)
    }

    val start = System.currentTimeMillis()
    val plans = schedulingPolicy.plan(tasks, candidates,
      HDMServerContext.defaultContext.SCHEDULING_FACTOR_CPU,
      HDMServerContext.defaultContext.SCHEDULING_FACTOR_IO ,
      HDMServerContext.defaultContext.SCHEDULING_FACTOR_NETWORK)
    val end = System.currentTimeMillis() - start
    totalScheduleTime.addAndGet(end)

    val scheduledTasks = blockingQue.filter(t => plans.contains(t.taskId)).map(t => t.taskId -> t).toMap[String, ParallelTask[_]]
    val now = System.currentTimeMillis()
    plans.foreach(tuple => {
      scheduledTasks.get(tuple._1) match {
        case Some(task) =>
          blockingQue.remove(task)
          scheduleTask(task, tuple._2.toString)
          val coreIdx = if (coreIdxMap.contains(tuple._2)) {
            coreIdxMap.get(tuple._2).get
          } else {
            0
          }
          // trace task
          val eTrace = ExecutionTrace(task.taskId,
            task.appId,
            task.version,
            task.exeId,
            task.func.getClass.getSimpleName,
            task.func.toString,
            task.input.map(_.id),
            Seq(task.taskId),
            tuple._2.toString,
            coreIdx,
            task.dep.toString,
            task.partitioner.getClass.getCanonicalName,
            now,
            -1L,
            "Running")
          historyManager.addExecTrace(eTrace)
        case None => //do nothing
      }
    })
  }


  def initStateScheduling(): Unit ={
    stageQueue.clear()
    appStateBuffer.clear()
    remoteTaskQueue.clear()
  }

  override def startup(): Unit = {
    isRunning.set(true)
    while (isRunning.get) try {
      if(taskQueue.isEmpty) {
        nonEmptyLock.acquire()
      }
      resAccessorlock.acquire()
      resourceManager.waitForNonEmpty()
      resourceManager.childrenRWLock.readLock().lock()
      val candidates = Scheduler.getAllAvailableWorkersWithIdx(resourceManager.getAllResources())
      resourceManager.childrenRWLock.readLock().unlock()
      scheduleOnResource(taskQueue, candidates)
      resAccessorlock.release()
    }
  }



  def startLocalTaskScheduling(): Unit = {
    isRunning.set(true)
    while (isRunning.get) try {
      if(localTaskQueue.isEmpty) {
        nonEmptyLock.acquire()
      }
      resAccessorlock.acquire()
      resourceManager.waitForChildrenNonEmpty()
      resourceManager.childrenRWLock.readLock().lock()
      val candidates = Scheduler.getAllAvailableWorkersWithIdx(resourceManager.getChildrenRes())
      resourceManager.childrenRWLock.readLock().unlock()
      scheduleOnResource(localTaskQueue, candidates)
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

      var candidates = Scheduler.getAllAvailableWorkers(resourceManager.getChildrenRes())
      while(candidates == null || candidates.isEmpty){
        log.info(s"Waiting for resources...")
        resourceManager.waitForChildrenNonEmpty()
        resourceManager.childrenRWLock.readLock().lock()
        candidates = Scheduler.getAllAvailableWorkers(resourceManager.getChildrenRes())
        resourceManager.childrenRWLock.readLock().unlock()
      }
      log.info(s"Completed waiting for resources...")
      // todo use scheduling policy to choose optimal candidates : scheduleOnResource(remoteTaskQueue, candidates)
      val start = System.currentTimeMillis()
      val workerPath = Scheduler.findClosestWorker(task, candidates).toString
      val end = System.currentTimeMillis() - start
      totalScheduleTime.addAndGet(end)

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
    hDMContext.compute(hdm, parallelism)
  }

  def addJobStages(appId:String, states:Seq[JobStage]): Future[HDM[_]] = {
    val promise = promiseManager.createPromise[HDM[_]](appId)
    states.map{ sg =>
      if(sg.parents == null || sg.parents.isEmpty){
        stageQueue.offer(sg)
      } else {
        appStateBuffer.getOrElseUpdate(sg.appId, new CopyOnWriteArrayList[JobStage]()) += sg
      }
    }
    historyManager.addJobStages(appId, states)
    promise.future
  }

  def startStateScheduling(): Unit ={
    while(isRunning.get()) {
      val stage = stageQueue.take()
      val hdm = stage.job
      val appName = hdm.appContext.appName
      val version = hdm.appContext.version
      val exeId = dependencyManager.addInstance(appName, version , hdm)
      dependencyManager.historyManager.addExeStage(stage.jobId, exeId)
      blockManager.addRef(hdm)
      val jobFuture = if(stage.isLocal){
        //if job is local
        val start = System.currentTimeMillis()
        val plans = planner.plan(hdm, stage.parallelism)
        dependencyManager.addPlan(exeId, plans)
        val end = System.currentTimeMillis() - start
        totalScheduleTime.addAndGet(end)
        submitJob(appName, version, exeId, plans.physicalPlan)
      } else {
        // send to remote master state based on context
        runRemoteJob(hdm, stage.parallelism)
      }
      jobFuture.onComplete {
        case Success(resHDM) => {
            log.info(s"A Stage succeed with ${hdm.id}")
            jobSucceed(appName + "#" + version, hdm.id, resHDM.blocks)
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
    log.info(s"Job succeeded with Scheduling time:${totalScheduleTime.get()} ms.")
    // trigger following jobs
    triggerStage(appId, ref)

  }

  def triggerStage(appId:String, hdm:HDM[_]): Unit = {
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
      nextStages.foreach{stage =>
        stages.remove(stage)
        stageQueue.offer(stage)
      }
    } else {
      log.info(s"A Job succeed id: ${appId}")
      val promise = promiseManager.removePromise(appId)
      if ((promise ne null) && (!promise.isCompleted)) {
        promise.asInstanceOf[Promise[HDM[_]]].success(hdm)
      }
      if(appStateBuffer.containsKey(appId)){
        appStateBuffer.remove(appId)
      }
    }
  }


  def getJobStage(appID:String):Seq[JobStage] = {
    appStateBuffer(appID)
  }


  def getAllApplications(): Seq[String] ={
    appStateBuffer.keySet().toIndexedSeq
  }

}
