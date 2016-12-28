package org.hdm.core.coordinator

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import org.hdm.akka.actors.worker.WorkActor
import org.hdm.core.executor.{BlockContext, ParallelTask, ClusterExecutor, HDMContext}
import org.hdm.core.message._
import org.hdm.core.server.DependencyManager
import org.hdm.core.storage.HDMBlockManager

import scala.beans.BeanProperty
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 *
 *
 *
 *
 * @param leaderPath
 */
class HDMClusterWorkerActor(var leaderPath: String, slots:Int, blockContext: BlockContext) extends WorkActor {

  def this(params:HDMWorkerParams) {
    this(params.master, params.slots, params.blockContext)
  }

  implicit val executorService: ExecutionContext = HDMContext.defaultHDMContext.executionContext

  val dependencyManager = DependencyManager()

  val runningTasks = new AtomicInteger(0)

  override def initParams(params: Any): Int = {
    super.initParams(params)
    context.actorSelection(leaderPath) ! JoinMsg(self.path.toString, slots)
    1
  }

  /**
   *
   * process business message
   */
  override def process: PartialFunction[Any, Unit] = {

    case scheduleMsg: SchedulingMsg => processSchedulingMsg(scheduleMsg)

    case msg: DependencyMsg => processDepMsg(msg)

    case msg: CoordinatingMsg => processCoordinationMsg(msg)

    case x => unhandled(x)
  }

  override def postStop(): Unit = {
    super.postStop()
    context.actorSelection(leaderPath) ! LeaveMsg(self.path.toString)
  }

  def processTask(task:ParallelTask[_]): Unit = {
    log.info(s"received a task: ${task.taskId + "_" + task.func}")
    runningTasks.incrementAndGet()
    val startTime = System.currentTimeMillis()
    val future = if(!HDMContext.defaultHDMContext.mockExecution) {
      ClusterExecutor.runTask(task.setBlockContext(this.blockContext))
    } else {
      ClusterExecutor.runMockTask(task.setBlockContext(this.blockContext))
    }
    future onComplete {
      case Success(results) =>
        context.actorSelection(leaderPath) ! TaskCompleteMsg(task.appId, task.taskId, task.func.toString, results)
        log.info(s"A task [${task.taskId + "_" + task.func}] has been completed in ${System.currentTimeMillis() - startTime} ms.")
        log.info(s"TaskCompleteMsg has been sent to ${leaderPath}.")
        log.info(s"Memory remanding: ${HDMBlockManager.freeMemMB} MB.")
      //recycle memory when workers are free
      //          if(runningTasks.decrementAndGet() <= 0) Future {
      //              Thread.sleep(64)
      //              HDMBlockManager.forceGC()
      //          }
      case Failure(t) =>
        t.printStackTrace()
        sender ! Seq.empty[Seq[String]]
      //          if(runningTasks.decrementAndGet() <= 0) {
      //            HDMBlockManager.forceGC()
      //          }
    }
  }

  protected def processSchedulingMsg: PartialFunction[SchedulingMsg, Unit] = {

    case AddTaskMsg(task) =>
      processTask(task)
    case SerializedTaskMsg(appName, version, taskId, serTask) =>
      val loader = dependencyManager.getClassLoader(appName, version)
      val task = HDMContext.DEFAULT_SERIALIZER.deserialize[ParallelTask[_]](ByteBuffer.wrap(serTask), loader)
      processTask(task)
  }


  protected def processDepMsg: PartialFunction[DependencyMsg, Unit] = {
    case AddApplication(appName, version, content, author) =>
      dependencyManager.submit(appName, version, content, author, false)
      log.info(s"received application bytes [$appName#$version]")

    case AddDependency(appName, version, depName, content, author) =>
      dependencyManager.addDep(appName, version, depName, content, author, false)
      log.info(s"received dependency [$depName] for application: [$appName#$version]")
  }


  protected def processCoordinationMsg: PartialFunction[CoordinatingMsg, Unit] = {
    case MigrationMsg(workerPath, toMaster) =>
      leaderPath = toMaster
      context.actorSelection(leaderPath) ! JoinMsg(self.path.toString, slots)
      //todo reset the heartbeat actor for this worker with the new master path
  }
}


case class HDMWorkerParams(@BeanProperty master:String,
                           @BeanProperty slots:Int,
                           @BeanProperty blockContext: BlockContext) extends ActorParams