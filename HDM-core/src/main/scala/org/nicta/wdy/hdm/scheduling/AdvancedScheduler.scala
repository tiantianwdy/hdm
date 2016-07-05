package org.nicta.wdy.hdm.scheduling

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import akka.actor.ActorSystem
import akka.pattern._
import akka.util.Timeout
import org.nicta.wdy.hdm.executor._
import org.nicta.wdy.hdm.functions.{DualInputFunction, ParUnionFunc, ParallelFunction}
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.message.SerializedTaskMsg
import org.nicta.wdy.hdm.model._
import org.nicta.wdy.hdm.server.provenance.ExecutionTrace
import org.nicta.wdy.hdm.server.{PromiseManager, ProvenanceManager, ResourceManager}
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager}
import org.nicta.wdy.hdm.utils.Logging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Lock, ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.collection.JavaConversions._


/**
 * Created by tiantian on 1/09/15.
 */
class AdvancedScheduler(val blockManager:HDMBlockManager,
                        val promiseManager:PromiseManager,
                        val resourceManager: ResourceManager,
                        val historyManager: ProvenanceManager,
                        val actorSys:ActorSystem,
                        val schedulingPolicy:SchedulingPolicy)(implicit val executorService:ExecutionContext) extends Scheduler with Logging{

  implicit val timeout = Timeout(5L, TimeUnit.MINUTES)

  //  private val workingSize = new Semaphore(0)

  protected val isRunning = new AtomicBoolean(false)

  protected val nonEmptyLock = new Semaphore(0)

  protected val resAccessorlock = new Lock

  protected val taskQueue = new LinkedBlockingQueue[ParallelTask[_]]()

  protected val appBuffer: java.util.Map[String, CopyOnWriteArrayList[ParallelTask[_]]] = new ConcurrentHashMap[String, CopyOnWriteArrayList[ParallelTask[_]]]()


  protected def scheduleOnResource(blockingQue:BlockingQueue[ParallelTask[_]], candidates:Seq[Path]): Unit ={

    val tasks = blockingQue.map { task =>
      val ids = task.input.map(_.id)
      val inputLocations = HDMBlockManager().getLocations(ids)
      val inputSize = HDMBlockManager().getblockSizes(ids).map(_ / 1024)
      SchedulingTask(task.taskId, inputLocations, inputSize, task.dep)
    }.toSeq

    val plans = schedulingPolicy.plan(tasks, candidates,
      HDMContext.defaultHDMContext.SCHEDULING_FACTOR_CPU,
      HDMContext.defaultHDMContext.SCHEDULING_FACTOR_IO ,
      HDMContext.defaultHDMContext.SCHEDULING_FACTOR_NETWORK)

    val scheduledTasks = blockingQue.filter(t => plans.contains(t.taskId)).map(t => t.taskId -> t).toMap[String,ParallelTask[_]]
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

  override def startup(): Unit = {
    isRunning.set(true)
    while (isRunning.get) try {
      if(taskQueue.isEmpty) {
        nonEmptyLock.acquire()
      }
      resAccessorlock.acquire()
      resourceManager.waitForNonEmpty()
      val candidates = Scheduler.getAllAvailableWorkers(resourceManager.getAllResources())
      scheduleOnResource(taskQueue, candidates)
      resAccessorlock.release()
    }
  }


  override def stop(): Unit = {
    isRunning.set(false)
  }

  override def init(): Unit = {
    isRunning.set(false)
    taskQueue.clear()
/*    synchronized[Unit]{
      nonEmptyLock.notifyAll()
    }*/
    val totalSlots = resourceManager.getAllResources().map(_._2.get()).sum
    resourceManager.release(totalSlots)
  }

  override def addTask[R](task: ParallelTask[R]): Promise[HDM[R]] = {
    val promise = promiseManager.createPromise[HDM[R]](task.taskId)
    if (!appBuffer.containsKey(task.appId))
      appBuffer.put(task.appId, new CopyOnWriteArrayList[ParallelTask[ _]])
    val lst = appBuffer.get(task.appId)
    lst += task
    triggerTasks(task.appId) //todo replace with planner.nextPlanning
    promise
  }

  override def submitJob(appId: String, version:String, exeId:String, hdms: Seq[HDM[_]]): Future[HDM[_]] = {
    val taskSeq = hdms.map { h => h match {
      case hdm: ParHDM[_, _] =>
        blockManager.addRef(h)
        val task = Task(appId = appId,
          version = version,
          exeId = exeId,
          taskId = h.id,
          input = h.children.asInstanceOf[Seq[ParHDM[_, hdm.inType.type]]],
          func = h.func.asInstanceOf[ParallelFunction[hdm.inType.type, hdm.outType.type]],
          dep = h.dependency,
          idx = hdm.index,
          partitioner = h.partitioner.asInstanceOf[Partitioner[hdm.outType.type]],
          appContext = hdm.appContext,
          blockContext = HDMContext.defaultHDMContext.blockContext())
        addTask(task)

      case dualDFM: DualDFM[_, _ ,_] =>
        blockManager.addRef(dualDFM)
        val task = new TwoInputTask(appId = appId,
          version = version,
          exeId = exeId,
          taskId = h.id,
          input1 = dualDFM.input1.asInstanceOf[Seq[HDM[dualDFM.inType1.type]]],
          input2 = dualDFM.input2.asInstanceOf[Seq[HDM[dualDFM.inType2.type]]],
          func = dualDFM.func.asInstanceOf[DualInputFunction[dualDFM.inType1.type, dualDFM.inType2.type, dualDFM.outType.type]],
          dep = h.dependency,
          idx = dualDFM.index,
          partitioner = h.partitioner.asInstanceOf[Partitioner[dualDFM.outType.type]],
          appContext = dualDFM.appContext,
          blockContext = HDMContext.defaultHDMContext.blockContext())
        addTask(task)
      }
    }
    val promise = taskSeq.last
    log.info(s"Created job promise Id: " + promise)
    promise.future
  }


  override def taskSucceeded(appId: String, taskId: String, func: String, blks: Seq[HDM[_]]): Unit = {

    val ref = blockManager.getRef(taskId) match {
      case dfm: DFM[_, _] =>
        val blkSeq = blks.flatMap(_.blocks)
        val children = blks.asInstanceOf[Seq[ParHDM[_, dfm.inType.type]]]
//        dfm.copy(blocks = blks, state = Computed)
        DFM(children,
          taskId,
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
    val endTime = System.currentTimeMillis()
    val serRef = ref.toSerializable()
//    HDMContext.declareHdm(Seq(ref))
    log.info(s"A task is succeeded : [${taskId + "_" + func}}] ")
    val promise = promiseManager.removePromise(taskId).asInstanceOf[Promise[HDM[_]]]
    if (promise != null && !promise.isCompleted ){
      promise.success(serRef)
      log.info(s"A promise [${promise}] is triggered for : [${taskId + "_" + func}}] ")
    } else if (promise eq null) {
      log.warn(s"no matched promise found: ${taskId}")
    }
    triggerTasks(appId)
    // update task trace
    val trace = historyManager.getExecTrace(taskId)
    if(trace ne null){
      val newTrace = if(blks ne null) trace.copy(outputPath= blks.map(_.toURL), endTime = endTime, status = "Completed")
      else trace.copy(endTime = endTime, status = "Completed")
      historyManager.updateExecTrace(newTrace)
    }
  }


  override protected def scheduleTask[R: ClassTag](task: ParallelTask[R], workerPath:String): Promise[HDM[R]] = {
    val promise = promiseManager.getPromise(task.taskId).asInstanceOf[Promise[HDM[R]]]


    if (task.func.isInstanceOf[ParUnionFunc[_]]) {
      //copy input blocks directly
      val blks = task.input.map(h => blockManager.getRef(h.id))
      taskSucceeded(task.appId, task.taskId, task.func.toString, blks)
    } else {
      // run job, assign to remote or local node to execute this task
      val updatedTask = task match {
        case singleInputTask:Task[_,R] =>
          val blkSeq = singleInputTask.input.map(h => blockManager.getRef(h.id)).flatMap(_.blocks)
          val inputDDMs = blkSeq.map(bl => blockManager.getRef(Path(bl).name))
          singleInputTask.asInstanceOf[Task[singleInputTask.inType.type, R]]
            .copy(input = inputDDMs.asInstanceOf[Seq[ParHDM[_, singleInputTask.inType.type]]])
        case twoInputTask:TwoInputTask[_, _, R] =>
          val blkSeq1 = twoInputTask.input1.map(h => blockManager.getRef(h.id)).flatMap(_.blocks)
          val blkSeq2 = twoInputTask.input2.map(h => blockManager.getRef(h.id)).flatMap(_.blocks)
          val inputDDM1 = blkSeq1.map(bl => blockManager.getRef(Path(bl).name))
          val inputDDM2 = blkSeq2.map(bl => blockManager.getRef(Path(bl).name))
          twoInputTask.asInstanceOf[TwoInputTask[twoInputTask.inTypeOne.type, twoInputTask.inTypeTwo.type, R]]
            .copy(input1 = inputDDM1.asInstanceOf[Seq[ParHDM[_, twoInputTask.inTypeOne.type]]], input2 = inputDDM2.asInstanceOf[Seq[ParHDM[_, twoInputTask.inTypeTwo.type]]])
      }
//      resourceManager.require(1)
      resourceManager.decResource(workerPath, 1)
      log.info(s"Task has been assigned to: [$workerPath] [${task.taskId + "_" + task.func.toString}}] ")
      val future = if (Path.isLocal(workerPath)) ClusterExecutor.runTask(updatedTask)
      else runRemoteTask(workerPath, updatedTask)

    }
    log.info(s"A task has been scheduled: [${task.taskId + "_" + task.func.toString}}] ")
    promise
  }


  protected def runRemoteTask[ R: ClassTag](workerPath: String, task: ParallelTask[R]): Future[Seq[String]] = {
    val taskBytes = HDMContext.DEFAULT_SERIALIZER.serialize(task).array
    val msg = SerializedTaskMsg(task.appId, task.version, task.taskId, taskBytes)
//    val msg = AddTaskMsg(task)
    val future = (actorSys.actorSelection(workerPath) ? msg).mapTo[Seq[String]]
    future
  }



  /**
   * find next tasks which are available to be executed
   * @param appId
   */
  private def triggerTasks(appId: String) = { //todo replace with planner.findNextTask
    if (appBuffer.containsKey(appId)) synchronized {
      val seq = appBuffer.get(appId)
        if (!seq.isEmpty) {
          //find tasks that all inputs have been computed
          val tasks = seq.filter(t =>
            if (t.input == null || t.input.isEmpty) false
            else try {
              t.input.forall{in =>
                val hdm = HDMBlockManager().getRef(in.id)
                if(hdm ne null)
                  hdm.state.eq(Computed)
                else false
              }
            } catch {
              case ex: Throwable => log.error(s"Got exception on ${t}"); false
            }
          )
          if ((tasks ne null) && !tasks.isEmpty) {
            seq --= tasks
            tasks.foreach( t =>
              if (t.func.isInstanceOf[ParUnionFunc[_]]) {
                //copy input blocks directly
                val blks = t.input.map(h => blockManager.getRef(h.id))
                taskSucceeded(t.appId, t.taskId, t.func.toString, blks)
              } else {
                taskQueue.put(t)
              }
            )
              nonEmptyLock.release()
            log.info(s"New tasks have has been triggered: [${tasks.map(t => (t.taskId, t.func)) mkString (",")}}] ")
          }
        }
    }

  }
}

