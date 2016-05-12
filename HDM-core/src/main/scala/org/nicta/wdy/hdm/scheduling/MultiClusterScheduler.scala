package org.nicta.wdy.hdm.scheduling

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import akka.actor.ActorSystem
import com.baidu.bpit.akka.server.SmsSystem
import org.nicta.wdy.hdm.executor.{Partitioner, ParallelTask}
import org.nicta.wdy.hdm.functions.ParallelFunction
import org.nicta.wdy.hdm.model.{DDM, ParHDM, DFM, HDM}
import org.nicta.wdy.hdm.planing.{MultiClusterPlaner, JobStage}
import org.nicta.wdy.hdm.server._
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

  private val appBuffer: java.util.Map[String, ListBuffer[JobStage]] = new ConcurrentHashMap[String, ListBuffer[JobStage]]()

  def initStateScheduling(): Unit ={
    stateQueue.clear()
    appBuffer.clear()
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
    hdm.compute(parallelism)
  }

  def addJobStages(states:Seq[JobStage]): Future[HDM[_]] = {
    states.map{ sg =>
      val promise = promiseManager.createPromise[HDM[_]](sg.jobId)
      if(sg.parents == null || sg.parents.isEmpty){
        stateQueue.offer(sg)
      } else {
        appBuffer.getOrElseUpdate(sg.appId, new ListBuffer[JobStage]()) += sg
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
        val ref = runRemoteJob(hdm, stage.parallelism)
        ref.onComplete{
          case Success(resHDM) =>
            jobSucceed(appName+"#"+version, resHDM.id, resHDM.children)
          case Failure(f) =>
        }
        // send to remote master state based on context
      }

    }
  }

  def jobSucceed(appId:String, jobId:String, blks:Seq[HDM[_]]): Unit = {
    // update hdm status
    val ref = blockManager.getRef(jobId) match {
      case dfm: DFM[_, _] =>
        val blkSeq = blks.flatMap(_.blocks)
        val children = blks.asInstanceOf[Seq[ParHDM[_, dfm.inType.type]]]
        //        dfm.copy(blocks = blks, state = Computed)
        DFM(children,
          jobId,
          dfm.dependency,
          dfm.func.asInstanceOf[ParallelFunction[dfm.inType.type, dfm.outType.type]],
          blkSeq,
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
    val stages  = appBuffer.get(appId)
    if(stages != null && stages.nonEmpty){
       val nextStages = stages.filter{ sg =>
        sg.parents.forall{ job =>
            val ref = blockManager.getRef(job.jobId)
            ref.state == Computed
        }
      }
      nextStages.foreach(stateQueue.offer(_))
    }
  }



}
