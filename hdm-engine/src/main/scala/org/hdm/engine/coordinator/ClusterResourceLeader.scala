package org.hdm.core.coordinator

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import akka.util.Timeout
import org.hdm.core.context.{HDMServerContext, HDMContext}
import org.hdm.engine.server.HDMEngine

import scala.collection.JavaConversions._
import akka.pattern._
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}
import org.hdm.akka.actors.worker.WorkActor
import org.hdm.akka.server.SmsSystem
import org.hdm.core.executor.ExecutorLauncher
import org.hdm.core.io.Path
import org.hdm.core.message._
import org.hdm.core.server.{ServerBackend, SingleResourceManager}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration.{Duration, DurationInt}


/**
  * Created by tiantian on 10/05/17.
  */
class ClusterResourceLeader(hdmBackend:ServerBackend, val hDMContext:HDMServerContext, val clusterMode:String = "single-cluster", val appMasterMode:String = "stand-alone") extends WorkActor
  with DepMsgReceiver
  with SchedulingMsgReceiver
  with CoordinationReceiver
  with RemotingEventManager {

  implicit val timeout = Timeout(600 seconds)
  implicit val maxWaitResponseTime = Duration(600, TimeUnit.SECONDS)
  implicit val executorService: ExecutionContext = hDMContext.executionContext

  val clusterResourceManager = new SingleResourceManager

  /**
    *  maintain the addresses of the application master for each application
    */
  private val appMasterMap:scala.collection.mutable.Map[String, String] = new ConcurrentHashMap[String, String]()

  def selfPath = self.path.toStringWithAddress(SmsSystem.localAddress)


  def this() {
    this(HDMEngine().getServerBackend(), HDMServerContext.defaultContext)
  }



  override def initParams(params: Any): Int = {
    super.initParams(params)
    //initiate hdm backend server
    hdmBackend.init()
    if(appMasterMode == "stand-alone"){
      val appMaster = initAppMaster()
      appMasterMap += ("_DEFAULT" -> appMaster)
      SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost", "org.hdm.core.coordinator.BlockManagerLeader", null)
      SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost", "org.hdm.core.coordinator.ResultHandler", null)
    }
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    1
  }

  def initAppMaster(): String ={
    val masterCls = if (clusterMode == "multi-cluster") "org.hdm.core.coordinator.HDMMultiClusterLeader"
    else "org.hdm.core.coordinator.SingleCoordinationLeader"
    SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost", masterCls, 0)
    val appMaster = self.path.parent.toStringWithAddress(SmsSystem.localAddress)
    appMaster
  }


  /**
    * process business message
    */
  override def process: PartialFunction[Any, Unit] = {

    case msg:ClusterMsg =>  processClusterMsg(msg)

    case jobMsg:SchedulingMsg => processScheduleMsg(jobMsg)

    case depMsg:DependencyMsg => processDepMsg(depMsg)

    case cdMsg:CoordinatingMsg => processCoordinationMsg(cdMsg)

    case event:RemotingLifecycleEvent => processRemotingEvents(event)
  }

  def processClusterMsg: PartialFunction[ClusterMsg, Unit] = {

    case InitAppMaster(host, port, cores, mem) =>

    case ShutdownMaster(graceful, shutdownWorkers) =>

    case InitExecutorMsg(master, core, mem, port, blockServerPort) =>

    case ShutdownExecutor(id, graceful) =>

    case restartExecutors => //restart all the executors on each slave node

  }


  override def processCoordinationMsg: PartialFunction[CoordinatingMsg, Unit] = {
    // coordinating msg
    case JoinMsg(path, state) =>
      val senderPath = sender().path.toString
      //      if (!followerMap.containsKey(senderPath))
      clusterResourceManager.addResource(senderPath, state)
      log.info(s"A worker has joined from [${senderPath}}] ")

    case LeaveMsg(nodes) =>
      nodes foreach {node =>
        clusterResourceManager.removeResource(node)
      }
      log.info(s"Executors have left the cluster from [${nodes}}] ")
  }



  override def processRemotingEvents: PartialFunction[RemotingLifecycleEvent, Unit] = {
    case event:DisassociatedEvent =>
      val affectedRes = clusterResourceManager.getAllResources().filter{ tup =>
        val path = Path(tup._1)
        Try {path.host == event.remoteAddress.host.get && path.port == event.remoteAddress.port.get} getOrElse false
      }.map(_._1)
      log.info(s"Remove disconnected resources ${affectedRes.mkString("[", ",", "]")}")
      affectedRes.foreach(res => clusterResourceManager.removeResource(res))

    case other: RemotingLifecycleEvent => unhandled(other)
  }

  override def processDepMsg: PartialFunction[DependencyMsg, Unit] = {
    case AddApplication(appName, version, content, author) =>
      hdmBackend.submitApplicationBytes(appName, version, content, author)
      clusterResourceManager.getAllResources().map(_._1) foreach { slave =>
        if (slave != selfPath) {
          log.info(s"sending application [$appName#$version] to $slave ...")
          context.actorSelection(slave) ! AddApplication(appName, version, content, author)
        }
      }

    case AddDependency(appName, version, depName, content, author) =>
      hdmBackend.addDep(appName, version, depName, content, author)
      clusterResourceManager.getAllResources().map(_._1) foreach { slave =>
        if (slave != selfPath) {
          log.info(s"sending dependency [$depName] of [$appName#$version] to $slave ...")
          context.actorSelection(slave) ! AddDependency(appName, version, depName, content, author)
        }
      }
  }

  override def processScheduleMsg: PartialFunction[SchedulingMsg, Unit] = {
    case msg: SerializedJobMsg =>
      //  init app master
      log.info(s"Received a job ${msg.appName}#${msg.version}.")
      val executionMaster = if(appMasterMode == "stand-alone") {
        log.info(s"Use default app master for job ${msg.appName}#${msg.version}.")
        if(! appMasterMap.contains("_DEFAULT")) {
          val master = initAppMaster()
          appMasterMap += ("_DEFAULT" -> master)
        }
        appMasterMap("_DEFAULT")
      } else {
        val HDM_HOME = hDMContext.HDM_HOME
        val appId = msg.appName + "#" + msg.version
        val launcher = ExecutorLauncher()
        val res = launcher.launchAppMaster(HDM_HOME, appId, "localhost", 8998)
        appMasterMap.put(appId, res._1)
        res._1
      }

      //  init app executor
      val slaves = clusterResourceManager.getAllResources().filter(_._2 > 0).keys
      val futureSeq = slaves.map{ slaveAddress =>
        val future = context.actorSelection(slaveAddress) ? InitExecutorMsg(appMaster = executionMaster, core = clusterResourceManager.getResource(slaveAddress)._2)
        log.info(s"Init executor on $slaveAddress.")
        future.mapTo[Any]
      }
      Future.sequence(futureSeq) onComplete {
        case Success(any) =>
          //  redirect to App master
          log.info(s"Initiated executor successfully. Redirecting job to appMaster...")
          context.actorSelection(executionMaster + "/" + HDMContext.CLUSTER_EXECUTOR_NAME).tell(msg, sender())

        case Failure(msg) =>
          log.error(s"Fails to initiate executors due to [$msg] .")
      }


    case ApplicationShutdown(appId:String) =>
      if(appMasterMode == "stand-alone") {
        // restart all slave executors
        val slaves = clusterResourceManager.getAllResources().keys
        val futureSeq = slaves.map { slaveAddress =>
          val future = context.actorSelection(slaveAddress) ? ShutdownExecutor("_DEFAULT", true)
          future.mapTo[Any]
        }
        Future.sequence(futureSeq) onComplete {
          case Success(any) =>
            //  clear resource manager for execution
//            hdmBackend.resourceManager.removeResource()
            log.info(s"Recycled executors for application [$appId] successfully.")
          case Failure(msg) =>
            log.error(s"Fails to shutdown executors for application [$appId] due to [$msg] .")
        }
      } else {
        // shutdown AppMaster and it will automatically shutdown its workers.
        val executionMaster = appMasterMap.get(appId).map{ master =>
          context.actorSelection(master) ! ShutdownMaster(false, true)
        }
      }

    case other: Any => unhandled(other)
  }


}


