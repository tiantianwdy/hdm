package org.hdm.core.coordinator

import java.util.concurrent.ConcurrentHashMap

import org.hdm.core.executor.ExecutorLauncher
import org.hdm.core.message._

import scala.collection.JavaConversions._

import org.hdm.akka.actors.worker.WorkActor

/**
  * Created by tiantian on 10/05/17.
  */
class ClusterResourceWorker(var leaderPath: String, slots:Int, mem:String, val hdmHome:String) extends WorkActor {

  private val  executors:scala.collection.mutable.Map[String, Process] = new ConcurrentHashMap[String, Process]()
  private val  executorSpec:scala.collection.mutable.Map[String, InitExecutorMsg] = new ConcurrentHashMap[String, InitExecutorMsg]()
  private val  executorLauncher = ExecutorLauncher()

  def this(params: ClusterResourceWorkerSpec) {
    this(params.leader, params.slots, params.mem, params.hdmHome)
  }

  override def initParams(params: Any): Int = {
    super.initParams(params)
    context.actorSelection(leaderPath) ! JoinMsg(self.path.toString, slots)
    1
  }
  /**
    * process business message
    */
  override def process: PartialFunction[Any, Unit] = {
    case InitExecutorMsg(appMaster, core, _, port, blockServerPort) => {
      executorSpec += ("_DEFAULT" -> InitExecutorMsg(appMaster, slots, mem, port, blockServerPort))
      log.info(s"launch executor to [$appMaster] from [$hdmHome] with cores:$core, memory:$mem .")
      val process = executorLauncher.launchExecutor(hdmHome, appMaster, "localhost", port, core, mem, blockServerPort)
      log.info(s"launching executor succeeded.")
      executors += ("_DEFAULT" -> process)
      sender() ! 0 // return success state
    }

    case ShutdownExecutor(id, gracefully) => {
      executors.remove(id) map { process =>
        if(gracefully){
          log.info(s"Shutting down executor with ID [$id] gracefully.")
          process.destroy()
        } else {
          log.info(s"Shutting down executor with ID [$id] forcibly.")
          process.destroyForcibly()
        }
      }
      log.info(s"Shutting down executor with ID [$id] succeeded.")
      sender() ! 0 // return success state
    }

    case RestartExecutors => {
      val executorIDs = executors.keys
      executorIDs.foreach{ id =>
        executors.remove(id).map(process => process.destroyForcibly())
        executorSpec.get(id).map { spec =>
          val process = executorLauncher.launchExecutor(hdmHome, spec.appMaster, "localhost", spec.port, spec.core, spec.mem, spec.blockServerPort)
          executors += ("_DEFAULT" -> process)
        }
        sender() ! 0
      }
    }

    case other:Any => log.warning(s"Unhandled msg: [$other]"); unhandled(other)

  }

  protected def processCoordinationMsg: PartialFunction[CoordinatingMsg, Unit] = {
    case MigrationMsg(workerPath, toMaster) =>
      leaderPath = toMaster
      context.actorSelection(leaderPath) ! JoinMsg(self.path.toString, slots)
    //todo reset the heartbeat actor for this worker with the new master path
  }

  /**
    * shutdown all the executors
    */
  def shutdownExecutors(): Unit ={
    executors.map(_._2).foreach(_.destroyForcibly())
  }

  override def postStop(): Unit = {
    log.info(s"Shutting down executors for worker...")
    executorSpec.clear()
    shutdownExecutors()
    log.info(s"Shutting down executors successfully..")
  }
}

case class ClusterResourceWorkerSpec(leader:String, slots:Int, mem:String, hdmHome:String) extends Serializable
