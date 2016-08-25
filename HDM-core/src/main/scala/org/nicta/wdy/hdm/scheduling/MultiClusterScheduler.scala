package org.nicta.wdy.hdm.scheduling

import java.util.concurrent._

import akka.actor.ActorSystem
import org.nicta.wdy.hdm.executor.{ClusterExecutor, HDMContext, Partitioner, ParallelTask}
import org.nicta.wdy.hdm.functions.ParallelFunction
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.{DDM, ParHDM, DFM, HDM}
import org.nicta.wdy.hdm.planing.{MultiClusterPlaner, JobStage}
import org.nicta.wdy.hdm.server._
import org.nicta.wdy.hdm.server.provenance.ExecutionTrace
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager}

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
                            val hDMContext: HDMContext)
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

  private val appStateBuffer: java.util.Map[String, CopyOnWriteArrayList[JobStage]] = new ConcurrentHashMap[String, CopyOnWriteArrayList[JobStage]]()

  protected override def scheduleOnResource(blockingQue:BlockingQueue[ParallelTask[_]], candidates:Seq[Path]): Unit = {

    val tasks= mutable.Buffer.empty[SchedulingTask]
    blockingQue.foreach{ task =>
      val ids = task.input.map(_.id)
      val inputLocations = new ArrayBuffer[Path](task.input.length)
      val inputSize = new ArrayBuffer[Long](task.input.length)
      inputLocations ++= HDMBlockManager().getLocations(ids)
      inputSize ++= HDMBlockManager().getblockSizes(ids).map(_ / 1024)
      tasks += SchedulingTask(task.taskId, inputLocations, inputSize, task.dep)
    }

//    val tasks = blockingQue.map { task =>
//      val ids = task.input.map(_.id)
//      val inputLocations = HDMBlockManager().getLocations(ids)
//      val inputSize = HDMBlockManager().getblockSizes(ids).map(_ / 1024)
//      SchedulingTask(task.taskId, inputLocations, inputSize, task.dep)
//    }.toSeq

    val start = System.currentTimeMillis()
    val plans = schedulingPolicy.plan(tasks, candidates,
      HDMContext.defaultHDMContext.SCHEDULING_FACTOR_CPU,
      HDMContext.defaultHDMContext.SCHEDULING_FACTOR_IO ,
      HDMContext.defaultHDMContext.SCHEDULING_FACTOR_NETWORK)
    val end = System.currentTimeMillis() - start
    totalScheduleTime.addAndGet(end)

    val scheduledTasks = blockingQue.filter(t => plans.contains(t.taskId)).map(t => t.taskId -> t).toMap[String, ParallelTask[_]]
    val now = System.currentTimeMillis()
    plans.foreach(tuple => {
      scheduledTasks.get(tuple._1) match {
        case Some(task) =>
          blockingQue.remove(task)
          scheduleTask(task, tuple._2)
          // trace task
          val eTrace = ExecutionTrace(task.taskId,
            task.appId,
            task.version,
            task.exeId,
            task.func.getClass.getSimpleName,
            task.func.toString,
            task.input.map(_.id),
            Seq(task.taskId),
            tuple._2,
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
    stateQueue.clear()
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
      val candidates = Scheduler.getAllAvailableWorkers(resourceManager.getAllResources())
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
      val candidates = Scheduler.getAllAvailableWorkers(resourceManager.getChildrenRes())
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
        stateQueue.offer(sg)
      } else {
        appStateBuffer.getOrElseUpdate(sg.appId, new CopyOnWriteArrayList[JobStage]()) += sg
      }
    }
    promise.future
  }

  def startStateScheduling(): Unit ={
    while(isRunning.get()){
      val stage = stateQueue.take()
      val hdm = stage.job
      val appName = hdm.appContext.appName
      val version = hdm.appContext.version
      val exeId = dependencyManager.addInstance(appName, version , hdm)
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
        stateQueue.offer(stage)
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



}
