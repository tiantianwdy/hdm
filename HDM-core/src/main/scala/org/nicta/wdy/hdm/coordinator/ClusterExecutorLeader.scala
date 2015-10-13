package org.nicta.wdy.hdm.coordinator

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{Semaphore, TimeUnit, LinkedBlockingQueue, ConcurrentHashMap}

import akka.actor.ActorPath
import com.baidu.bpit.akka.monitor.SystemMonitorService
import com.baidu.bpit.akka.server.SmsSystem
import org.apache.commons.logging.LogFactory
import org.nicta.wdy.hdm.Buf
import org.nicta.wdy.hdm.scheduling.Scheduler
import org.nicta.wdy.hdm.utils.Logging
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

import akka.pattern.{ask, pipe}
import akka.util.Timeout

import com.baidu.bpit.akka.actors.worker.WorkActor

import org.nicta.wdy.hdm.executor._
import org.nicta.wdy.hdm.model._
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager}
import org.nicta.wdy.hdm.io.{HDMIOManager, DataParser, AkkaIOManager, Path}
import org.nicta.wdy.hdm.message._
import org.nicta.wdy.hdm.functions.{ParUnionFunc, ParallelFunction}
import org.nicta.wdy.hdm.message.AddTaskMsg
import scala.util.Failure
import org.nicta.wdy.hdm.message.AddHDMsMsg
import org.nicta.wdy.hdm.message.JoinMsg
import scala.util.Success
import org.nicta.wdy.hdm.message.LeaveMsg

/**
 * Created by Tiantian on 2014/12/18.
 */
@deprecated(message = "replaced by HDMClusterLeaderActor", since = "0.0.1")
class ClusterExecutorLeader(cores:Int) extends WorkActor with Scheduler {

  import scala.collection.JavaConversions._

  implicit val executorService: ExecutionContext = HDMContext.executionContext

  implicit val timeout = Timeout(5L, TimeUnit.MINUTES)

  val blockManager = HDMBlockManager()

//  val ioManager = new AkkaIOManager

  val appManager = new AppManager

  val appBuffer: java.util.Map[String, ListBuffer[Task[_, _]]] = new ConcurrentHashMap[String, ListBuffer[Task[_, _]]]()

  val taskQueue = new LinkedBlockingQueue[Task[_, _]]()

  val promiseMap = new ConcurrentHashMap[String, Promise[_]]()

  /**
   * maintain the state or free slots of each follower
   */
  val followerMap: java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]

  val workingSize = new Semaphore(0)

  val isRunning = new AtomicBoolean(false)





  override def initParams(params: Any): Int = {
    super.initParams(params)
    followerMap.put(self.path.toStringWithAddress(SmsSystem.localAddress).toString, new AtomicInteger(cores))
    log.info(s"Leader has been initiated with $cores cores.")
    init()
    new Thread{
      override def run(): Unit = {
        startup()
      }
    }.start()
    1
  }

  /**
   *
   * process business message
   */
  override def process: PartialFunction[Any, Unit] = {
    // task management msg
    case AddTaskMsg(task) =>
      addTask(task).future onComplete {
        case Success(hdm) => sender ! hdm
        case Failure(t) => sender ! t.toString
      }
      log.info(s"A task has been added from [${sender.path}}]; id: ${task.taskId}} ")

     //deprecated, replaced by SubmitJobMsg
    case AddHDMsMsg(appId, hdms, resultHandler) =>
      val senderPath = sender.path
      val fullPath = ActorPath.fromString(resultHandler).toStringWithAddress(senderPath.address)
      submitJob(appId, hdms) onComplete {
        case Success(hdm) =>
          val resActor = context.actorSelection(fullPath)
          resActor ! JobCompleteMsg(appId, 1, hdm)
          //clean resources for execution
//          val app = appManager.getApp(appId)
//          val hdms = app.plan.foreach(hdm => HDMContext.removeBlock(hdm.id))
          appBuffer.remove(appId)
          log.info(s"A job has completed successfully. result has been send to [${resultHandler}}]; appId: ${appId}} ")
        case Failure(t) =>
          context.actorSelection(resultHandler) ! JobCompleteMsg(appId, 1, t.toString)
          log.info(s"A job has failed. result has been send to [${resultHandler}}]; appId: ${appId}} ")
      }
      log.info(s"A job has been added from [${sender.path}}]; id: ${appId}} ")


    case SubmitJobMsg(appId, hdm, resultHandler, parallel) =>
      val senderPath = sender.path
      val fullPath = ActorPath.fromString(resultHandler).toStringWithAddress(senderPath.address)
      jobReceived(appId, hdm, parallel) onComplete {
        case Success(hdm) =>
          val resActor = context.actorSelection(fullPath)
          resActor ! JobCompleteMsg(appId, 1, hdm)
          //clean resources for execution
          val app = appManager.getApp(appId)
//          val hdms = app.plan.foreach(hdm => HDMContext.removeBlock(hdm.id))
          appBuffer.remove(appId)
          log.info(s"A job has completed successfully. result has been send to [${resultHandler}}]; appId: ${appId}} ")
        case Failure(t) =>
          context.actorSelection(resultHandler) ! JobCompleteMsg(appId, 1, t.toString)
          log.info(s"A job has failed. result has been send to [${resultHandler}}]; appId: ${appId}} ")
      }
      log.info(s"A job has been added from [${sender.path}}]; id: ${appId}} ")



    case TaskCompleteMsg(appId, taskId, func, results) =>
      log.info(s"received a task completed msg: ${taskId + "_" + func}")
      val workerPath = sender.path.toString
      HDMContext.declareHdm(results, false)
      followerMap.get(workerPath).incrementAndGet()
      workingSize.release(1)
      taskSucceeded(appId, taskId, func, results)

    // coordinating msg
    case JoinMsg(path, state) =>
      val senderPath = sender().path.toString
//      if (!followerMap.containsKey(senderPath))
      followerMap.put(senderPath, new AtomicInteger(state))
      if(state > 0)
        workingSize.release(state)
      log.info(s"A executor has joined from [${senderPath}}] ")

    case LeaveMsg(senderPath) =>
      followerMap.remove(senderPath)
      log.info(s"A executor has left from [${senderPath}}] ")

    case x => unhandled(x); log.info(s"received a unhanded msg [${x}}] ")
  }


  override def postStop(): Unit = {
    super.postStop()
    stop()
  }

  override protected def scheduleTask[I: ClassTag, R: ClassTag](task: Task[I, R], worker:String): Promise[HDM[I, R]] = {
    val promise = promiseMap.get(task.taskId).asInstanceOf[Promise[HDM[I, R]]]


    if (task.func.isInstanceOf[ParUnionFunc[_]]) {
      //copy input blocks directly
//      val results = task.input.map(h => blockManager.getRef(h.id))
      val blks = task.input.map(h => blockManager.getRef(h.id))
      taskSucceeded(task.appId, task.taskId, task.func.toString, blks)
    } else {
      // run job, assign to remote or local node to execute this task
      val blks = task.input.map(h => blockManager.getRef(h.id)).flatMap(_.blocks)
      val inputDDMs = blks.map(bl => blockManager.getRef(Path(bl).name))
      val updatedTask = task.copy(input = inputDDMs.asInstanceOf[Seq[HDM[_, I]]])
      workingSize.acquire(1)
      var workerPath = findPreferredWorker(updatedTask)
      while (workerPath == null || workerPath == ""){ // wait for available workers
        log.info(s"no worker available for task[${task.taskId + "__" + task.func.toString}}] ")
        workerPath = findPreferredWorker(updatedTask)
        Thread.sleep(50)
      }
      log.info(s"Task has been assigned to: [$workerPath] [${task.taskId + "__" + task.func.toString}}] ")
      val future = if (Path.isLocal(workerPath)) ClusterExecutor.runTaskSynconized(updatedTask)
      else runRemoteTask(workerPath, updatedTask)

/*      future onComplete {
        case Success(blkUrls) =>
          taskSucceeded(task.appId, task.taskId,task. func.toString, blkUrls)
          followerMap.get(workerPath).incrementAndGet()
        case Failure(t) => println(t.toString)
      }*/
    }
    log.info(s"A task has been scheduled: [${task.taskId + "__" + task.func.toString}}] ")
    promise
  }

  override def stop(): Unit = {
    isRunning.set(false)
  }

  override def startup(): Unit = {
    isRunning.set(true)
    while (isRunning.get) {
      val task = taskQueue.take()
      log.info(s"A task has been scheduling: [${task.taskId + "__" + task.func.toString}}] ")
      scheduleTask(task, "")
    }
  }

  override def init(): Unit = {
    isRunning.set(false)
    taskQueue.clear()
    val totalSlots = followerMap.map(_._2.get()).sum
    workingSize.release(totalSlots)
  }

  override def addTask[I, R](task: Task[I, R]): Promise[HDM[I, R]] = {
    val promise = Promise[HDM[I, R]]()
    promiseMap.put(task.taskId, promise)
    if (!appBuffer.containsKey(task.appId))
      appBuffer.put(task.appId, new ListBuffer[Task[_, _]])
    val lst = appBuffer.get(task.appId)
    lst += task
    triggerTasks(task.appId)
    promise
  }

  def jobReceived(appId:String, hdm:HDM[_,_], parallelism:Int): Future[HDM[_, _]] = {
    appManager.addApp(appId, hdm)
    val plan = HDMContext.explain(hdm, parallelism)
    appManager.addPlan(appId, plan)
    submitJob(appId, plan)
  }

  //todo move and implement at job compiler
  override def submitJob(appId: String, hdms: Seq[HDM[_, _]]): Future[HDM[_, _]] = {
    hdms.map { h =>
      blockManager.addRef(h)
      val task = Task(appId = appId,
        taskId = h.id,
        input = h.children.asInstanceOf[Seq[HDM[_, h.inType.type]]],
        func = h.func.asInstanceOf[ParallelFunction[h.inType.type, h.outType.type]],
        dep = h.dependency,
        partitioner = h.partitioner.asInstanceOf[Partitioner[h.outType.type ]])
      addTask(task)
    }.last.future
  }



  def taskSucceeded(appId: String, taskId: String, func: String, blks: Seq[HDM[_, _]]): Unit = {

    val ref = HDMBlockManager().getRef(taskId) match {
      case dfm: DFM[_ , _] =>
        val blkSeq = blks.flatMap(_.blocks)
        dfm.copy(blocks = blkSeq, state = Computed)
      case ddm: DDM[_ , _] =>
        ddm.copy(state = Computed)
    }
    blockManager.addRef(ref)
    HDMContext.declareHdm(Seq(ref))
    log.info(s"A task is succeeded : [${taskId + "_" + func}}] ")
    val promise = promiseMap.remove(taskId).asInstanceOf[Promise[HDM[_, _]]]
    if (promise != null && !promise.isCompleted ){
      promise.success(ref.asInstanceOf[HDM[_, _]])
      log.info(s"A promise is triggered for : [${taskId + "_" + func}}] ")
    }
    else if (promise eq null) {
      log.warning(s"no matched promise found: ${taskId}")
      log.warning(s"current promiss map: ${promiseMap.keys().toSeq}")
    }
    triggerTasks(appId)
  }


  private def triggerTasks(appId: String) = {
    if (appBuffer.containsKey(appId)) {
      val seq = appBuffer.get(appId)
      synchronized {
        if (!seq.isEmpty) {
          //find tasks that all inputs have been computed
          val tasks = seq.filter(t =>
            if (t.input eq null) false
            else try {
              t.input.forall{in =>
                val hdm = HDMBlockManager().getRef(in.id)
                if(hdm ne null)
                  hdm.state.eq(Computed)
                else false
              }
            } catch {
              case ex: Throwable => log.error(ex, s"Got exception on ${t}"); false
            }
          )
          if ((tasks ne null) && !tasks.isEmpty) {
            seq --= tasks
            tasks.foreach(taskQueue.put(_))
            log.info(s"New tasks have has been triggered: [${tasks.map(t => (t.taskId, t.func)) mkString (",")}}] ")
          }
        }
      }
    }

  }

  private def runRemoteTask[I: ClassTag, R: ClassTag](workerPath: String, task: Task[I, R]): Future[Seq[String]] = {
    val future = (context.actorSelection(workerPath) ? AddTaskMsg(task)).mapTo[Seq[String]]
    future
  }



  private def findPreferredWorker(task: Task[_, _]): String = try {

    //    val inputLocations = task.input.flatMap(hdm => HDMBlockManager().getRef(hdm.id).blocks).map(Path(_))
    val inputLocations = task.input.flatMap { hdm =>
      val nhdm = HDMBlockManager().getRef(hdm.id)
      if (nhdm.preferLocation == null)
        nhdm.blocks.map(Path(_))
      else Seq(nhdm.preferLocation)
    }
    log.info(s"Block prefered input locations:${inputLocations.mkString(",")}")
    val candidates =
      if (task.dep == OneToN || task.dep == OneToOne) Scheduler.getAvailablePaths(followerMap) // for parallel tasks
      else Scheduler.getFreestWorkers(followerMap) // for shuffle tasks

    //find closest worker from candidates
    if (candidates.size > 0) {
      val workerPath = Path.findClosestLocation(candidates, inputLocations).toString
      followerMap.get(workerPath).decrementAndGet() // reduce slots of worker
      workerPath
    } else ""
  } catch {
    case e: Throwable => log.error(e, s"failed to find worker for task:${task.taskId}"); ""
  }

}




