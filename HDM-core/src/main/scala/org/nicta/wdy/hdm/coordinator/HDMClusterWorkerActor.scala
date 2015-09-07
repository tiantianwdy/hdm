package org.nicta.wdy.hdm.coordinator

import java.util.concurrent.atomic.AtomicInteger

import com.baidu.bpit.akka.actors.worker.WorkActor
import org.nicta.wdy.hdm.executor.{ClusterExecutor, HDMContext}
import org.nicta.wdy.hdm.message.{LeaveMsg, TaskCompleteMsg, AddTaskMsg, JoinMsg}

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
    case AddTaskMsg(task) =>
      log.info(s"received a task: ${task.taskId + "_" + task.func}")
      runningTasks.incrementAndGet()
      val startTime = System.currentTimeMillis()
      ClusterExecutor.runTaskSynconized(task) onComplete {
        case Success(results) =>
          context.actorSelection(leaderPath) ! TaskCompleteMsg(task.appId, task.taskId, task.func.toString, results)
          log.info(s"A task [${task.taskId + "_" + task.func}] has been completed in ${System.currentTimeMillis() - startTime} ms.")
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
    case x => unhandled(x)
  }

  override def postStop(): Unit = {
    super.postStop()
    context.actorSelection(leaderPath) ! LeaveMsg(self.path.toString)
  }

}
