package org.nicta.wdy.hdm.scheduling

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import akka.actor.ActorSystem
import org.nicta.wdy.hdm.executor.{HDMContext, Partitioner, ParallelTask}
import org.nicta.wdy.hdm.functions.ParallelFunction
import org.nicta.wdy.hdm.model.{DDM, ParHDM, DFM, HDM}
import org.nicta.wdy.hdm.planing.{MultiClusterPlaner, JobStage}
import org.nicta.wdy.hdm.server._
import org.nicta.wdy.hdm.server.provenance.ExecutionTrace
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

/**
 * Created by tiantian on 10/05/16.
 */
class MultiClusterScheduler(override val blockManager:HDMBlockManager,
                            override val promiseManager:PromiseManager,
                            override val resourceManager: TreeResourceManager,
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

  private val appStateBuffer: java.util.Map[String, ListBuffer[JobStage]] = new ConcurrentHashMap[String, ListBuffer[JobStage]]()

  override def startup(): Unit = {
    isRunning.set(true)
    while (isRunning.get) {
      if(taskQueue.isEmpty) {
        nonEmptyLock.acquire()
      }
      resourceManager.waitForNonEmpty()
      val candidates = Scheduler.getAllAvailableWorkers(resourceManager.getAllResources())
      import scala.collection.JavaConversions._

      val tasks = taskQueue.map { task =>
        val ids = task.input.map(_.id)
        val inputLocations = HDMBlockManager().getLocations(ids)
        val inputSize = HDMBlockManager().getblockSizes(ids).map(_ / 1024)
        SchedulingTask(task.taskId, inputLocations, inputSize, task.dep)
      }.toSeq

      val plans = schedulingPolicy.plan(tasks, candidates,
        HDMContext.defaultHDMContext.SCHEDULING_FACTOR_CPU,
        HDMContext.defaultHDMContext.SCHEDULING_FACTOR_IO ,
        HDMContext.defaultHDMContext.SCHEDULING_FACTOR_NETWORK)

      val scheduledTasks = taskQueue.filter(t => plans.contains(t.taskId)).map(t => t.taskId -> t).toMap[String,ParallelTask[_]]
      val now = System.currentTimeMillis()
      plans.foreach(tuple => {
        scheduledTasks.get(tuple._1) match {
          case Some(task) =>
            taskQueue.remove(task)
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
  }

  def initStateScheduling(): Unit ={
    stateQueue.clear()
    appStateBuffer.clear()
  }


  def scheduleRemoteTask[R: ClassTag](task: ParallelTask[R]): Promise[HDM[R]] = {
    val promise = promiseManager.createPromise[HDM[R]](task.taskId)
    val candidates = Scheduler.getAllAvailableWorkers(resourceManager.getChildrenRes())
    val workerPath = candidates.head.toString
    resourceManager.decResource(workerPath, 1)
    super.runRemoteTask(workerPath, task)
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
        blockManager.addRef(hdm)
        val ref = runRemoteJob(hdm, stage.parallelism)
        ref.onComplete{
          case Success(resHDM) =>
            jobSucceed(appName+"#"+version, hdm.id, resHDM.blocks)
          case Failure(f) =>
        }
        // send to remote master state based on context
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
    if(stages != null && stages.nonEmpty){
      val nextStages = stages.filter { sg =>
        sg.parents.forall { job =>
          val ref = blockManager.getRef(job.jobId)
          ref.state == Computed
        }
      }
      nextStages.foreach(stateQueue.offer(_))
    }
  }



}
