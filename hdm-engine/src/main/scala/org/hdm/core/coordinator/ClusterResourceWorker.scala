package org.hdm.core.coordinator

import java.util.concurrent.ConcurrentHashMap

import org.hdm.core.executor.ExecutorLauncher
import org.hdm.core.message._

import scala.collection.JavaConversions._
import org.hdm.akka.actors.worker.WorkActor
import org.hdm.akka.server.SmsSystem
import org.hdm.core.server.DependencyManager

/**
  * Created by tiantian on 10/05/17.
  */
class ClusterResourceWorker(var leaderPath: String, slots: Int, mem: String, val hdmHome: String) extends WorkActor {

  private val executors: scala.collection.mutable.Map[String, Process] = new ConcurrentHashMap[String, Process]()
  private val executorSpec: scala.collection.mutable.Map[String, InitExecutorMsg] = new ConcurrentHashMap[String, InitExecutorMsg]()
  private val executorLauncher = ExecutorLauncher()

  protected val dependencyManager = DependencyManager()

  def this(params: ClusterResourceWorkerSpec) {
    this(params.leader, params.slots, params.mem, params.hdmHome)
  }

  def selfPath = self.path.toStringWithAddress(SmsSystem.localAddress)

  override def initParams(params: Any): Int = {
    super.initParams(params)
    context.actorSelection(leaderPath) ! JoinMsg(selfPath, slots)
    1
  }

  private def initExecutor(id: String, appMaster: String, core: Int, port: Int, blockServerPort: Int): Unit = {
    executorSpec += (id -> InitExecutorMsg(appMaster, slots, mem, port, blockServerPort))
    log.info(s"launch executor [$id] to [$appMaster] from [$hdmHome] with cores:$core, memory:$mem .")
    val process = executorLauncher.launchExecutor(hdmHome, appMaster, "localhost", port, core, mem, blockServerPort)
    log.info(s"launching executor succeeded.")
    executors += (id -> process)
  }

  private def shutdownExecutor(id: String, gracefully: Boolean): Unit = {
    executors.remove(id) map { process =>
      if (gracefully) {
        log.info(s"Shutting down executor with ID [$id] gracefully.")
        process.destroy()
      } else {
        log.info(s"Shutting down executor with ID [$id] forcibly.")
        process.destroyForcibly()
      }
    }
    log.info(s"Shutting down executor with ID [$id] succeeded.")
  }

  /**
    * process business message
    */
  override def process: PartialFunction[Any, Unit] = {

    case msg: ClusterMsg => processClusterMsg(msg)

    case msg: DependencyMsg => processDepMsg(msg)

    case other: Any => log.warning(s"Unhandled msg: [$other]"); unhandled(other)

  }

  protected def processClusterMsg: PartialFunction[ClusterMsg, Unit] = {
    case InitExecutorMsg(appMaster, core, _, port, blockServerPort) => {
      if (!executors.contains("_DEFAULT")) {
        initExecutor("_DEFAULT", appMaster, core, port, blockServerPort)
      } else {
        if (executors("_DEFAULT").isAlive) {
          log.info(s"Executor exists, reuse existing executor.")
        } else {
          // process is dead.
          try {
            shutdownExecutor("_DEFAULT", false)
          }
          initExecutor("_DEFAULT", appMaster, core, port, blockServerPort)
        }
      }
      sender() ! 0 // return success state
    }

    case ShutdownExecutor(id, gracefully) => {
      shutdownExecutor(id, gracefully)
      sender() ! 0 // return success state
    }

    case RestartExecutors => {
      val executorIDs = executors.keys
      executorIDs.foreach { id =>
        executors.remove(id).map(process => process.destroyForcibly())
        executorSpec.get(id).map { spec =>
          val process = executorLauncher.launchExecutor(hdmHome, spec.appMaster, "localhost", spec.port, spec.core, spec.mem, spec.blockServerPort)
          executors += ("_DEFAULT" -> process)
        }
        sender() ! 0
      }
    }

    case other: Any => log.warning(s"Unhandled msg: [$other]"); unhandled(other)
  }

  protected def processCoordinationMsg: PartialFunction[CoordinatingMsg, Unit] = {
    case MigrationMsg(workerPath, toMaster) =>
      leaderPath = toMaster
      context.actorSelection(leaderPath) ! JoinMsg(self.path.toString, slots)
    //todo reset the heartbeat actor for this worker with the new master path
  }


  protected def processDepMsg: PartialFunction[DependencyMsg, Unit] = {
    case AddApplication(appName, version, content, author) =>
      dependencyManager.submit(appName, version, content, author, false)
      log.info(s"received application bytes [$appName#$version]")

    case AddDependency(appName, version, depName, content, author) =>
      dependencyManager.addDep(appName, version, depName, content, author, false)
      log.info(s"received dependency [$depName] for application: [$appName#$version]")
  }


  /**
    * shutdown all the executors
    */
  def shutdownExecutors(): Unit = {
    executors.map(_._2).foreach(_.destroyForcibly())
  }

  override def postStop(): Unit = {
    log.info(s"Shutting down executors for worker...")
    executorSpec.clear()
    shutdownExecutors()
    log.info(s"Shutting down executors successfully..")
    context.actorSelection(leaderPath) ! LeaveMsg(Seq(selfPath))
  }
}


/**
  *  A parameter object for initiation of a ClusterResourceWorker.
  * @param leader
  * @param slots
  * @param mem
  * @param hdmHome
  */
case class ClusterResourceWorkerSpec(leader: String, slots: Int, mem: String, hdmHome: String) extends Serializable
