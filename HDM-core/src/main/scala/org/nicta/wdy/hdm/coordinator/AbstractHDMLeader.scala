package org.nicta.wdy.hdm.coordinator

import com.baidu.bpit.akka.actors.worker.WorkActor
import com.baidu.bpit.akka.server.SmsSystem
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.message.{QueryMsg, DependencyMsg, CoordinatingMsg, SchedulingMsg}
import org.nicta.wdy.hdm.server.ServerBackend

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
                                                              with ClusterMsgReceiver {

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
      processClusterMsg(msg)

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
class SingleClusterLeader (override val hdmBackend:ServerBackend,
                           override val cores:Int ,
                           override val hDMContext:HDMContext)
                            extends AbstractHDMLeader(hdmBackend, cores, hDMContext)
                            with DefQueryMsgReceiver
                            with DefDepReceiver
                            with DefSchedulingReceiver
                            with SingleClusteringReceiver {

  def this(cores: Int) {
    this(HDMContext.defaultHDMContext.getServerBackend(), cores, HDMContext.defaultHDMContext)
  }

}