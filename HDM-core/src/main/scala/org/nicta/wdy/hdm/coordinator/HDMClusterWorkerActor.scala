package org.nicta.wdy.hdm.coordinator

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import com.baidu.bpit.akka.actors.worker.WorkActor
import org.nicta.wdy.hdm.executor.{ParallelTask, ClusterExecutor, HDMContext}
import org.nicta.wdy.hdm.message._
import org.nicta.wdy.hdm.server.DependencyManager
import org.nicta.wdy.hdm.storage.HDMBlockManager

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 *
 *
 *
 *
 * @param leaderPath
 */
class HDMClusterWorkerActor(leaderPath: String) extends WorkActor {


  implicit val executorService: ExecutionContext = HDMContext.executionContext

  val dependencyManager = DependencyManager()

  val runningTasks = new AtomicInteger(0)

  override def initParams(params: Any): Int = {
    super.initParams(params)
    context.actorSelection(leaderPath) ! JoinMsg(self.path.toString, HDMContext.slot.get())
    1
  }

  /**
   *
   * process business message
   */
  override def process: PartialFunction[Any, Unit] = {

    case scheduleMsg: SchedulingMsg => processSchedulingMsg(scheduleMsg)

    case msg: DependencyMsg => processDepMsg(msg)

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
    ClusterExecutor.runTask(task) onComplete {
      case Success(results) =>
        context.actorSelection(leaderPath) ! TaskCompleteMsg(task.appId, task.taskId, task.func.toString, results)
        log.info(s"A task [${task.taskId + "_" + task.func}] has been completed in ${System.currentTimeMillis() - startTime} ms.")
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
      val task = HDMContext.defaultSerializer.deserialize[ParallelTask[_]](ByteBuffer.wrap(serTask), loader)
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

}
