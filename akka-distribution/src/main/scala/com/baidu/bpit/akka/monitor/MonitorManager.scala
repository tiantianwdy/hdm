package com.baidu.bpit.akka.monitor

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import com.baidu.bpit.akka.messages.CollectMsg
import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import com.baidu.bpit.akka.messages.HeartbeatMsgResp
import com.baidu.bpit.akka.messages.JoinMsg
import com.baidu.bpit.akka.messages.MonitorMsg

trait MonitorManager extends Actor with Monitable with ActorLogging {
  val monitors: ListBuffer[AnyRef] = ListBuffer()
  var dataList: List[MonitorData] = List()

  def dispatchMonitorMsg: Receive = {
    case MonitorMsg(monitorData) => dataList ++= monitorData
    case CollectMsg => {
      sender ! MonitorMsg(getMonitorData())
      clearData()
    }
    case JoinMsg(_, slavePath) if slavePath != null && slavePath != "" => {
      val actor = context.system.actorSelection(slavePath)
      monitors += actor
    }
    case JoinMsg(_, _) => monitors += sender
    case HeartbeatMsgResp =>
    case _ => unhandled()
  }

  def getMonitorData(reset: Boolean = true) = {
    implicit val ec = context.dispatcher
    implicit val timeout = Timeout(5 seconds)
    monitors.map {
      case monitor: ActorRef => {
        val data = ask(monitor, CollectMsg).mapTo[MonitorMsg]
        data pipeTo self
      }
      case monitor: ActorSelection => {
        val data = ask(monitor, CollectMsg).mapTo[MonitorMsg]
        data pipeTo self
      }
      case monitor: Monitable => {
        dataList ++= monitor.getMonitorData(reset)
      }
    }
    log.debug(dataList.map(_.toString).mkString("\n"))
    dataList
  }

  def clearData() {
    dataList = List() // clean data
  }

  def registerMonitor(monitor: AnyRef) {
    monitors += monitor
  }
}
