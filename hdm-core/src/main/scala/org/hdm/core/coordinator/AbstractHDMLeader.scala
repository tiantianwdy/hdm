package org.hdm.core.coordinator

import org.hdm.akka.actors.worker.WorkActor
import org.hdm.akka.server.SmsSystem
import org.hdm.core.executor.HDMContext
import org.hdm.core.message.{QueryMsg, DependencyMsg, CoordinatingMsg, SchedulingMsg}
import org.hdm.core.server.ServerBackend

import scala.concurrent.ExecutionContext

/**
 * Created by tiantian on 9/05/16.
 */
abstract class AbstractHDMLeader (val hdmBackend:ServerBackend,
                                  val cores:Int ,
                                  val hDMContext:HDMContext) extends WorkActor
                                                              with QueryReceiver
                                                              with SchedulingMsgReceiver
                                                              with DepMsgReceiver
                                                              with CoordinationReceiver {

  implicit val executorService: ExecutionContext = hDMContext.executionContext

  def selfPath = self.path.toStringWithAddress(SmsSystem.localAddress).toString

  override def initParams(params: Any): Int = {
    super.initParams(params)
    //initiate hdm backend server
    hdmBackend.init()
    if(cores > 0){
      hdmBackend.resourceManager.addResource(self.path.toStringWithAddress(SmsSystem.localAddress).toString, cores)
    }
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
                               override val hDMContext:HDMContext)
                            extends AbstractHDMLeader(hdmBackend, cores, hDMContext)
                            with DefQueryMsgReceiver
                            with DefDepReceiver
                            with DefSchedulingReceiver
                            with SingleClustering {

  def this(cores: Int) {
    this(HDMContext.defaultHDMContext.getServerBackend(), cores, HDMContext.defaultHDMContext)
  }

}