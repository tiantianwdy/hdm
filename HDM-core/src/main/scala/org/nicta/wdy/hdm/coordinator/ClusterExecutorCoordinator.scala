package org.nicta.wdy.hdm.coordinator

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{TimeUnit, LinkedBlockingQueue, ConcurrentHashMap}

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

import akka.pattern.{ask, pipe}
import akka.util.Timeout

import com.baidu.bpit.akka.actors.worker.WorkActor

import org.nicta.wdy.hdm.executor.{Scheduler, Task}
import org.nicta.wdy.hdm.model.{DDM, DFM, HDM}
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager}
import org.nicta.wdy.hdm.io.{AkkaIOManager, Path}
import org.nicta.wdy.hdm.message._
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.functions.ParallelFunction
import org.nicta.wdy.hdm.message.AddTaskMsg
import scala.util.Failure
import org.nicta.wdy.hdm.message.AddJobMsg
import org.nicta.wdy.hdm.message.JoinMsg
import org.nicta.wdy.hdm.model.DFM
import scala.util.Success
import org.nicta.wdy.hdm.message.LeaveMsg
import org.nicta.wdy.hdm.executor.Task

/**
 * Created by Tiantian on 2014/12/18.
 */
class ClusterExecutorLeader() extends WorkActor with Scheduler{

  import scala.collection.JavaConversions._

  implicit val executorService:ExecutionContext = HDMContext.executionContext

  val blockManager = HDMBlockManager()

  val ioManager = new AkkaIOManager

  /**
   * maintain the state or free slots of each follower
   */
  val followerMap:java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]

  val appBuffer:java.util.Map[String, ListBuffer[Task[_,_]]] = new ConcurrentHashMap[String, ListBuffer[Task[_,_]]]()

  val taskQueue = new LinkedBlockingQueue[Task[_,_]]()

  val promiseMap = new ConcurrentHashMap[String, Promise[_]]()

  val isRunning = new AtomicBoolean(false)


  implicit val timeout = Timeout(10L, TimeUnit.MINUTES)


  override def initParams(params: Any): Int = {
    super.initParams(params)
    followerMap.put(self.path.parent.toString, new AtomicInteger(HDMContext.CORES))
    Future{
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
      submitJob(appId, hdms) onComplete {
        case Success(hdm) =>
          val resActor = context.actorSelection(resultHandler)
          resActor ! JobCompleteMsg(appId, 1, hdm)
          log.info(s"A job has completed successfully. result has been send to [${resultHandler}}]; appId: ${appId}} ")
        case Failure(t) =>
          context.actorSelection(resultHandler) ! JobCompleteMsg(appId, 1, t.toString)
          log.info(s"A job has failed. result has been send to [${resultHandler}}]; appId: ${appId}} ")
      }
      log.info(s"A job has been added from [${sender.path}}]; id: ${appId}} ")

    case JobCompleteMsg(appId, state, result) =>
      promiseMap

    // coordinating msg
    case JoinMsg(senderPath, state) =>
      if(!followerMap.containsKey(senderPath))
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

  override protected def scheduleTask[I:TypeTag, R:TypeTag](task: Task[I, R]): Promise[HDM[I, R]] = {
    val promise = promiseMap.get(task.taskId).asInstanceOf[Promise[HDM[I, R]]]
    // run job, assign to remote or local node to execute this task
    val workerPath = findPreferredWorker(task)
    val future = if(Path.isLocal(workerPath)) runTaskLocally(task)
                 else runRemoteTask(workerPath, task)

    future onComplete {
      case Success(blks) =>
        val ref = HDMBlockManager().getRef(task.taskId) match {
          case dfm:DFM[I,R] => dfm.copy(blocks = blks, state = Computed)
          case ddm:DDM[R] => ddm.copy(state = Computed)
        }
        HDMBlockManager().addRef(ref)
        promise.success(ref.asInstanceOf[HDM[I, R]])
        triggerApp(task.appId)

      case Failure(t) => println(t.toString)
    }
    log.info(s"A task has has been scheduled: [${task.taskId + "__" +  task.func.toString}}] ")
    promise
  }

  override def stop(): Unit = {
    isRunning.set(false)
  }

  override def start(): Unit = {
    isRunning.set(true)
    while(isRunning.get){
      val task = taskQueue.take()
      scheduleTask(task)
    }
  }

  override def init(): Unit = {
    isRunning.set(false)
    taskQueue.clear()
  }

  override def addTask[I, R](task: Task[I, R]): Promise[HDM[I,R]] = {
    val promise = Promise[HDM[I,R]]()
    promiseMap.put(task.taskId, promise)
    if(!appBuffer.containsKey(task.appId))
      appBuffer.put(task.appId, new ListBuffer[Task[_,_]])
    val lst = appBuffer.get(task.appId)
    lst += task
    triggerApp(task.appId)
    promise
  }

  //todo move and implement at job compiler
  override def submitJob(appId:String, hdms:Seq[HDM[_,_]]): Future[HDM[_,_]] = {
    hdms.map{h =>
      blockManager.addRef(h)
      val task = Task(appId = appId,
        taskId = h.id,
        input = h.children.asInstanceOf[Seq[HDM[_, h.inType.type]]],
        func = h.func.asInstanceOf[ParallelFunction[h.inType.type, h.outType.type]])
      addTask(task)
    }.last.future
  }



  private def triggerApp(appId:String) = {
    if(appBuffer.containsKey(appId)){
      val seq = appBuffer.get(appId)
      synchronized{
        if(!seq.isEmpty) {
          //find tasks that all inputs have been computed
          val tasks = seq.filter(t =>
            if(t.input eq null) false
            else {
              t.input.forall(in =>
                HDMBlockManager().getRef(in.id).state.eq(Computed))
            }
          )
          if((tasks ne null) && !tasks.isEmpty){
            seq --= tasks
            tasks.foreach(taskQueue.put(_))
            log.info(s"New tasks have has been triggered: [${tasks.map(t => (t.taskId, t.func))mkString(",")}}] ")
          }
        }
      }
    }

  }


  // private methods for task scheduling

  private def runTaskLocally [I:TypeTag, R:TypeTag](task: Task[I, R]): Future[Seq[String]] = {
    // prepare input data from cluster
    log.info(s"Preparing input data for task: [${(task.taskId, task.func)}] ")
    val futureSeq = task.input.flatMap(_.blocks).filterNot(id => Path.isLocal(id)).map{remoteId =>
       ioManager.askBlock(remoteId, remoteId)
    }
    if(futureSeq != null && !futureSeq.isEmpty)
      Future.sequence(futureSeq.asInstanceOf[Seq[Future[String]]]) map {ids =>
        log.info(s"Input data preparing finished, the task starts running: [${(task.taskId, task.func)}] ")
        task.call().map(_.id)
      }
    else Future {
      log.info(s"All data are at local, the task starts running: [${(task.taskId, task.func)}] ")
      task.call().map(bl => bl.id)
    }
  }

  private def runRemoteTask [I:TypeTag, R:TypeTag](workerPath:String,  task: Task[I, R]): Future[Seq[String]] = {
    val future = (context.actorSelection(workerPath) ? AddTaskMsg(task)).mapTo[Seq[String]]
    future
  }

  private def getAvailableWorks():Seq[Path] = {

    followerMap.filter(t => t._2.get() > 0).map(s => Path(s._1)).toSeq
  }

  private def findPreferredWorker(task: Task[_,_]): String = {

    val inputLocations = task.input.flatMap(hdm => HDMBlockManager().getRef(hdm.id).blocks).map(Path(_))
    val availableWorkers = getAvailableWorks()
    //find closest worker which has positive slot in flower map
    val workerPath = findClosestLocation(inputLocations, availableWorkers)
    followerMap.get(workerPath).decrementAndGet() // reduce slots of worker
    workerPath
  }

  private def findClosestLocation(paths: Seq[Path], fromSet:Seq[Path]): String = {
    //todo implement distance function
    followerMap.keys.head
  }


}

/**
 *
 * @param leaderPath
 */
class ClusterExecutorFollower(leaderPath:String) extends  WorkActor {


  implicit val executorService:ExecutionContext = HDMContext.executionContext

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
      Future {
        //todo load remote input data of this task
        task.call().map(bl => bl.id)
      } onComplete {
        case Success(blks) => sender ! blks
        case Failure(t) => log.error(t.toString); sender ! Seq.empty[Seq[String]]
      }

    case x => unhandled(x)
  }

  override def postStop(): Unit = {
    super.postStop()
    context.actorSelection(leaderPath) ! LeaveMsg(self.path.toString)
  }

}
