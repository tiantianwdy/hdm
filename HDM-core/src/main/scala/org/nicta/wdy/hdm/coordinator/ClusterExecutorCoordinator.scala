package org.nicta.wdy.hdm.coordinator

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue, ConcurrentHashMap}

import akka.actor.ActorPath
import org.apache.commons.logging.LogFactory
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

import akka.pattern.{ask, pipe}
import akka.util.Timeout

import com.baidu.bpit.akka.actors.worker.WorkActor

import org.nicta.wdy.hdm.executor._
import org.nicta.wdy.hdm.model.{DDM, DFM, HDM}
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager}
import org.nicta.wdy.hdm.io.{DataParser, AkkaIOManager, Path}
import org.nicta.wdy.hdm.message._
import org.nicta.wdy.hdm.functions.{ParUnionFunc, ParallelFunction}
import org.nicta.wdy.hdm.message.AddTaskMsg
import scala.util.Failure
import org.nicta.wdy.hdm.message.AddJobMsg
import org.nicta.wdy.hdm.message.JoinMsg
import org.nicta.wdy.hdm.model.DFM
import scala.util.Success
import org.nicta.wdy.hdm.message.LeaveMsg

/**
 * Created by Tiantian on 2014/12/18.
 */
class ClusterExecutorLeader() extends WorkActor with Scheduler {

  import scala.collection.JavaConversions._

  implicit val executorService: ExecutionContext = HDMContext.executionContext

  val blockManager = HDMBlockManager()

  val ioManager = new AkkaIOManager

  /**
   * maintain the state or free slots of each follower
   */
  val followerMap: java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]

  val appBuffer: java.util.Map[String, ListBuffer[Task[_, _]]] = new ConcurrentHashMap[String, ListBuffer[Task[_, _]]]()

  val taskQueue = new LinkedBlockingQueue[Task[_, _]]()

  val promiseMap = new ConcurrentHashMap[String, Promise[_]]()

  val isRunning = new AtomicBoolean(false)


  implicit val timeout = Timeout(10L, TimeUnit.MINUTES)


  override def initParams(params: Any): Int = {
    super.initParams(params)
    followerMap.put(self.path.toString, new AtomicInteger(HDMContext.CORES))
    Future {
      start()
    }
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

    case AddJobMsg(appId, hdms, resultHandler) =>
      val senderPath = sender.path
      val fullPath = ActorPath.fromString(resultHandler).toStringWithAddress(senderPath.address)
      submitJob(appId, hdms) onComplete {
        case Success(hdm) =>
          val resActor = context.actorSelection(fullPath)
          resActor ! JobCompleteMsg(appId, 1, hdm)
          log.info(s"A job has completed successfully. result has been send to [${resultHandler}}]; appId: ${appId}} ")
        case Failure(t) =>
          context.actorSelection(resultHandler) ! JobCompleteMsg(appId, 1, t.toString)
          log.info(s"A job has failed. result has been send to [${resultHandler}}]; appId: ${appId}} ")
      }
      log.info(s"A job has been added from [${sender.path}}]; id: ${appId}} ")

    case TaskCompleteMsg(appId, taskId, func, result) =>
      log.info(s"received a task completed msg: ${taskId + "_" + func}")
      val workerPath = sender.path.toString
      taskSucceeded(appId, taskId, func, result)
      followerMap.get(workerPath).incrementAndGet()
    // coordinating msg
    case JoinMsg(path, state) =>
      val senderPath = sender().path.toString
      if (!followerMap.containsKey(senderPath))
        followerMap.put(senderPath, new AtomicInteger(state))
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

  override protected def scheduleTask[I: ClassTag, R: ClassTag](task: Task[I, R]): Promise[HDM[I, R]] = {
    val promise = promiseMap.get(task.taskId).asInstanceOf[Promise[HDM[I, R]]]
    if (task.func.isInstanceOf[ParUnionFunc[_]]) {
      //copy input blocks directly
      val blks = task.input.map(h => blockManager.getRef(h.id)).flatMap(_.blocks)
      taskSucceeded(task.appId, task.taskId,task. func.toString, blks)
    } else {
      // run job, assign to remote or local node to execute this task
      var workerPath = findPreferredWorker(task)
      while (workerPath == null || workerPath == ""){ // wait for available workers
        workerPath = findPreferredWorker(task)
        Thread.sleep(100)
      }
      val future = if (Path.isLocal(workerPath)) ClusterExecutor.runTaskLocally(task)
      else runRemoteTask(workerPath, task)

      future onComplete {
        case Success(blkUrls) =>
          taskSucceeded(task.appId, task.taskId,task. func.toString, blkUrls)
          followerMap.get(workerPath).incrementAndGet()
        case Failure(t) => println(t.toString)
      }
    }
    log.info(s"A task has been scheduled: [${task.taskId + "__" + task.func.toString}}] ")
    promise
  }

  override def stop(): Unit = {
    isRunning.set(false)
  }

  override def start(): Unit = {
    isRunning.set(true)
    while (isRunning.get) {
      val task = taskQueue.take()
      log.info(s"A task has been scheduling: [${task.taskId + "__" + task.func.toString}}] ")
      scheduleTask(task)
    }
  }

  override def init(): Unit = {
    isRunning.set(false)
    taskQueue.clear()
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

  //todo move and implement at job compiler
  override def submitJob(appId: String, hdms: Seq[HDM[_, _]]): Future[HDM[_, _]] = {
    hdms.map { h =>
      blockManager.addRef(h)
      val task = Task(appId = appId,
        taskId = h.id,
        input = h.children.asInstanceOf[Seq[HDM[_, h.inType.type]]],
        func = h.func.asInstanceOf[ParallelFunction[h.inType.type, h.outType.type]],
        partitioner = h.partitioner.asInstanceOf[Partitioner[h.outType.type ]])
      addTask(task)
    }.last.future
  }

  private def taskSucceeded(appId:String, taskId:String, func:String, blks: Seq[String]): Unit = {
    val promise = promiseMap.get(taskId).asInstanceOf[Promise[HDM[_, _]]]
    val ref = HDMBlockManager().getRef(taskId) match {
      case dfm: DFM[_, _] => dfm.copy(blocks = blks, state = Computed)
      case ddm: DDM[_] => ddm.copy(state = Computed)
    }
    HDMBlockManager().addRef(ref)
    if (!promise.isCompleted)
      promise.success(ref.asInstanceOf[HDM[_, _]])
    triggerTasks(appId)
    log.info(s"A task is succeeded: [${taskId + "_" + func}}] ")
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
              case _ => false
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


  // private methods for task scheduling

  private def runTaskLocally[I: ClassTag, R: ClassTag](task: Task[I, R]): Future[Seq[String]] = {
    // prepare input data from cluster
    log.info(s"Preparing input data for task: [${(task.taskId, task.func)}] ")
    val input = task.input.map(in => blockManager.getRef(in.id)) // update the states of input blocks
    val updatedTask = task.copy(input = input.asInstanceOf[Seq[HDM[_, I]]])
    val unComputed = input.flatMap(_.blocks).map(Path(_)).filterNot(p => blockManager.isCached(p.name))
    val futureBlocks = unComputed.map(p => blockManager.getRef(p.name)).map { ddm =>
      ddm.location.protocol match {
        //todo replace with using data parsers
        case Path.AKKA =>
          ioManager.askBlock(ddm.location.name, ddm.location.parent) // this is only for hdm
        case Path.HDFS => Future {
          val bl = DataParser.readBlock(ddm.location)
          blockManager.add(ddm.id, bl)
          ddm.id
        }
      }
    }

    if (futureBlocks != null && !futureBlocks.isEmpty)
      Future.sequence(futureBlocks.asInstanceOf[Seq[Future[String]]]) map { ids =>
        log.info(s"Input data preparing finished, the task starts running: [${(task.taskId, task.func)}] ")
        updatedTask.call().map(_.toURL)
      }
    else Future {
      log.info(s"All data are at local, the task starts running: [${(task.taskId, task.func)}] ")
      updatedTask.call().map(bl => bl.toURL)
    }
  }

  private def runRemoteTask[I: ClassTag, R: ClassTag](workerPath: String, task: Task[I, R]): Future[Seq[String]] = {
    val future = (context.actorSelection(workerPath) ? AddTaskMsg(task)).mapTo[Seq[String]]
    future
  }

  private def getAvailableWorks(): Seq[Path] = {

    followerMap.filter(t => t._2.get() > 0).map(s => Path(s._1)).toSeq
  }

  private def findPreferredWorker(task: Task[_, _]): String = {

    val inputLocations = task.input.flatMap(hdm => HDMBlockManager().getRef(hdm.id).blocks).map(Path(_))
    val availableWorkers = getAvailableWorks()
    //find closest worker which has positive slot in flower map
    if(availableWorkers.size > 0){
      val workerPath = Path.findClosestLocation(availableWorkers, inputLocations).toString
      followerMap.get(workerPath).decrementAndGet() // reduce slots of worker
      workerPath
    } else ""
  }

}

/**
 *
 *
 *
 *
 * @param leaderPath
 */
class ClusterExecutorFollower(leaderPath: String) extends WorkActor {


  implicit val executorService: ExecutionContext = HDMContext.executionContext

  override def initParams(params: Any): Int = {
    super.initParams(params)
    context.actorSelection(leaderPath) ! JoinMsg(self.path.toString, HDMContext.CORES)
    1
  }

  /**
   *
   * process business message
   */
  override def process: PartialFunction[Any, Unit] = {
    case AddTaskMsg(task) =>
      log.info(s"received a task: ${task.taskId + "_" + task.func}")
      val senderPath = sender.path
      ClusterExecutor.runTaskLocally(task) onComplete {
        case Success(blkUrls) =>
          context.actorSelection(leaderPath) ! TaskCompleteMsg(task.appId, task.taskId, task.func.toString, blkUrls)
          log.info(s"A task has been completed: ${task.taskId + "_" + task.func}")
        case Failure(t) => log.error(t.toString); sender ! Seq.empty[Seq[String]]
      }

    case x => unhandled(x)
  }

  override def postStop(): Unit = {
    super.postStop()
    context.actorSelection(leaderPath) ! LeaveMsg(self.path.toString)
  }

}

object ClusterExecutor {

  val log = LoggerFactory.getLogger(ClusterExecutor.getClass)


  val blockManager = HDMBlockManager()

  val ioManager = new AkkaIOManager

  def runTaskLocally[I: ClassTag, R: ClassTag](task: Task[I, R])(implicit executionContext: ExecutionContext): Future[Seq[String]] = {
    // prepare input data from cluster
    log.info(s"Preparing input data for task: [${(task.taskId, task.func)}] ")
    val input = task.input.map(in => blockManager.getRef(in.id)) // update the states of input blocks
    val updatedTask = task.copy(input = input.asInstanceOf[Seq[HDM[_, I]]])
    val unComputed = input.flatMap(_.blocks).map(Path(_)).filterNot(p => blockManager.isCached(p.name))
    val futureBlocks = unComputed.map(p => blockManager.getRef(p.name)).map { ddm =>
      ddm.location.protocol match {
        //todo replace with using data parsers
        case Path.AKKA =>
          ioManager.askBlock(ddm.location.name, ddm.location.parent) // this is only for hdm
        case Path.HDFS => Future {
          val bl = DataParser.readBlock(ddm.location)
          log.info(s"Output data size: ${bl.size} ")
          blockManager.add(ddm.id, bl)
          ddm.id
        }
      }
    }

    if (futureBlocks != null && !futureBlocks.isEmpty)
      Future.sequence(futureBlocks.asInstanceOf[Seq[Future[String]]]) map { ids =>
        log.info(s"Input data preparing finished, the task starts running: [${(updatedTask.taskId, updatedTask.func)}] ")
        val ids = updatedTask.call().map(_.toURL)
        log.info(s"Task completed, with output id: [${ids}] ")
        ids
      }
    else Future {
      log.info(s"All data are at local, the task starts running: [${(updatedTask.taskId, updatedTask.func)}] ")
      updatedTask.call().map(bl => bl.toURL)
    }
  }
}

