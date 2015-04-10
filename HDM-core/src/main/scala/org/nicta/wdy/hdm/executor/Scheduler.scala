package org.nicta.wdy.hdm.executor

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{TimeUnit, ConcurrentHashMap, LinkedBlockingQueue}

import akka.pattern.ask
import akka.util.Timeout
import com.baidu.bpit.akka.actors.worker.WorkActor

import org.nicta.wdy.hdm.coordinator.ClusterExecutor
import org.nicta.wdy.hdm.functions.{ParUnionFunc, ParallelFunction}
import org.nicta.wdy.hdm.io.{AkkaIOManager, Path}
import org.nicta.wdy.hdm.message.AddTaskMsg
import org.nicta.wdy.hdm.model.{DDM, DFM, HDM}
import org.nicta.wdy.hdm.storage.{Computed, HDMBlockManager}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/**
  * Created by Tiantian on 2014/12/1.
 */
trait Scheduler {

  implicit val executorService:ExecutionContext

  def submitJob(appId:String, hdms:Seq[HDM[_,_]]): Future[HDM[_,_]]

  def addTask[I,R](task:Task[I,R]):Promise[HDM[I,R]]

  def taskSucceeded(appId:String, taskId:String, func:String, blks: Seq[String]): Unit

  def init()

  def start()

  def stop()

  protected def scheduleTask [I:ClassTag, R:ClassTag](task:Task[I,R]):Promise[HDM[I,R]]

}

class SimpleActorBasedScheduler(implicit val executorService:ExecutionContext) extends Scheduler{

  this: WorkActor =>

  import scala.collection.JavaConversions._


  val blockManager = HDMBlockManager()

  val ioManager = new AkkaIOManager

  val appManager = new AppManager

  val appBuffer: java.util.Map[String, ListBuffer[Task[_, _]]] = new ConcurrentHashMap[String, ListBuffer[Task[_, _]]]()

  val taskQueue = new LinkedBlockingQueue[Task[_, _]]()

  val promiseMap = new ConcurrentHashMap[String, Promise[_]]()

  /**
   * maintain the state or free slots of each follower
   */
  val followerMap: java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]

  val isRunning = new AtomicBoolean(false)

  implicit val timeout = Timeout(5L, TimeUnit.MINUTES)

  override protected def scheduleTask[I: ClassTag, R: ClassTag](task: Task[I, R]): Promise[HDM[I, R]] = {
    val promise = promiseMap.get(task.taskId).asInstanceOf[Promise[HDM[I, R]]]
    val blks = task.input.map(h => blockManager.getRef(h.id)).flatMap(_.blocks)

    if (task.func.isInstanceOf[ParUnionFunc[_]]) {
      //copy input blocks directly
      //      val blks = task.input.map(h => blockManager.getRef(h.id))
      taskSucceeded(task.appId, task.taskId, task.func.toString, blks)
    } else {
      // run job, assign to remote or local node to execute this task
      val inputDDMs = blks.map(bl => blockManager.getRef(Path(bl).name))
      val updatedTask = task.copy(input = inputDDMs.asInstanceOf[Seq[HDM[_, I]]])
      var workerPath = findPreferredWorker(updatedTask)
      while (workerPath == null || workerPath == ""){ // wait for available workers
        log.info(s"no worker available for task[${task.taskId + "__" + task.func.toString}}] ")
        workerPath = findPreferredWorker(updatedTask)
        Thread.sleep(100)
      }
      log.info(s"Task has been assigned to: [$workerPath] [${task.taskId + "__" + task.func.toString}}] ")
      val future = if (Path.isLocal(workerPath)) ClusterExecutor.runTaskSynconized(updatedTask)
      else runRemoteTask(workerPath, updatedTask)

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



  def taskSucceeded(appId:String, taskId:String, func:String, blks: Seq[String]): Unit = {

    val ref = HDMBlockManager().getRef(taskId) match {
      case dfm: DFM[_ , _] => dfm.copy(blocks = blks, state = Computed)
      case ddm: DDM[_ , _] => ddm.copy(state = Computed)
    }
    blockManager.addRef(ref)
    HDMContext.declareHdm(Seq(ref))
    log.info(s"A task is succeeded : [${taskId + "_" + func}}] ")
    val promise = promiseMap.remove(taskId).asInstanceOf[Promise[HDM[_, _]]]
    if (promise != null && !promise.isCompleted){
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

  private def runRemoteTask[I: ClassTag, R: ClassTag](workerPath: String, task: Task[I, R]): Future[Seq[String]] = {
    val future = (context.actorSelection(workerPath) ? AddTaskMsg(task)).mapTo[Seq[String]]
    future
  }

  private def getAvailableWorks(): Seq[Path] = {

    followerMap.filter(t => t._2.get() > 0).map(s => Path(s._1)).toSeq
  }


  private def findPreferredWorker(task: Task[_, _]): String = try {

    //    val inputLocations = task.input.flatMap(hdm => HDMBlockManager().getRef(hdm.id).blocks).map(Path(_))
    val inputLocations = task.input.flatMap{ hdm =>
      val nhdm = HDMBlockManager().getRef(hdm.id)
      if(nhdm.preferLocation == null)
        nhdm.blocks.map(Path(_))
      else Seq(nhdm.preferLocation)
    }
    log.info(s"Block prefered input locations:${inputLocations.mkString(",")}")
    val availableWorkers = getAvailableWorks()
    //find closest worker which has positive slot in flower map
    if(availableWorkers.size > 0){
      val workerPath = Path.findClosestLocation(availableWorkers, inputLocations).toString
      followerMap.get(workerPath).decrementAndGet() // reduce slots of worker
      workerPath
    } else ""
  } catch {
    case e:Throwable => log.error(e, s"failed to find worker for task:${task.taskId}"); ""
  }
}
