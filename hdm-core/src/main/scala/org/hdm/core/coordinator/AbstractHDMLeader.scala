package org.hdm.core.coordinator

import akka.remote.RemotingLifecycleEvent
import org.hdm.akka.actors.worker.WorkActor
import org.hdm.akka.server.SmsSystem
import org.hdm.core.message.{CoordinatingMsg, DependencyMsg, QueryMsg, SchedulingMsg}
import org.hdm.core.server.{HDMServerContext, ServerBackend}

import scala.concurrent.ExecutionContext

/**
 * Created by tiantian on 9/05/16.
 */
abstract class AbstractHDMLeader (val hdmBackend:ServerBackend,
                                  val cores:Int ,
                                  val hDMContext:HDMServerContext) extends WorkActor
                                                              with QueryReceiver
                                                              with SchedulingMsgReceiver
                                                              with DepMsgReceiver
                                                              with CoordinationReceiver
                                                              with RemotingEventManager {

  implicit val executorService: ExecutionContext = hDMContext.executionContext

  def selfPath = self.path.toStringWithAddress(SmsSystem.localAddress).toString

  override def initParams(params: Any): Int = {
    super.initParams(params)
    //initiate hdm backend server
    hdmBackend.init()
    if(cores > 0){
      hdmBackend.resourceManager.addResource(self.path.toStringWithAddress(SmsSystem.localAddress).toString, cores)
    }
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    log.info(s"Leader has been initiated with $cores cores.")
    1
  }

  /**
   *
   * process business message
   */
  override def process: PartialFunction[Any, Unit] = {

    case msg: SchedulingMsg =>
      log.debug(s"received a scheduling msg [${msg}}] ")
      processScheduleMsg(msg)

    case msg: CoordinatingMsg =>
      log.info(s"received a coordination msg [${msg}}] ")
      processCoordinationMsg(msg)

    case msg: DependencyMsg =>
      log.info(s"received a dependency msg [${msg}}] ")
      processDepMsg(msg)

    case msg: QueryMsg =>
      log.info(s"received a query msg [${msg}}] ")
      processQueries(msg)

    case msg:RemotingLifecycleEvent =>
      log.info(s"received a RemotingLifecycleEvent[${msg}}] ")
      processRemotingEvents(msg)

    case x: Any => log.info(s"received a unhanded msg [${x}}] ")
  }


  override def postStop(): Unit = {
    super.postStop()
    hdmBackend.shutdown()
  }

}


/**
 *
 * @param hdmBackend
 * @param cores
 * @param hDMContext
 */
class SingleCoordinationLeader(override val hdmBackend:ServerBackend,
                               override val cores:Int,
                               override val hDMContext:HDMServerContext)
                            extends AbstractHDMLeader(hdmBackend, cores, hDMContext)
                            with DefQueryMsgReceiver
                            with DefDepReceiver
                            with DefSchedulingReceiver
                            with SingleClustering
                            with RemoteExecutorMonitor {

  def this(cores: Int) {
    this(HDMServerContext.defaultContext.getServerBackend(), cores, HDMServerContext.defaultContext)
  }

}