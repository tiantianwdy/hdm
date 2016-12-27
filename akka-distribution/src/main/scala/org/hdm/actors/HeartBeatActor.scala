package org.hdm.akka.actors

import org.hdm.akka.messages.BasicMessage
import org.hdm.akka.messages.HeartbeatMsg
import org.hdm.akka.messages.InitMsg
import org.hdm.akka.messages.MasterSlaveProtocol
import org.hdm.akka.messages.StopMsg
import org.hdm.akka.monitor.MonitorDataExtension
import org.hdm.akka.monitor.SystemMonitor
import akka.actor.Actor
import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.actor.ActorLogging
import org.hdm.akka.monitor.MonitorManager


/** 
 *  心跳Actor实现
 * @author wudongyao
 * @date 2013-7-14 
 * @version 0.0.1
 *
 */
class HeartBeatActor(var masterPath:String, val isLinux:Boolean)  extends Actor with ActorLogging with MonitorManager{

  var duration: Long = 5000
  var isStopped: Boolean = false

  override def preStart() {
	registerMonitor(context.actorOf(Props(new SystemMonitor(isLinux)), "systemMonitor"))
	registerMonitor( MonitorDataExtension(context.system))
  }

  def sendHeartbeat(){
    context.actorSelection(masterPath) ! HeartbeatMsg(monitorData = getMonitorData())
    clearData()
  }

  def receive = {
    case msg: BasicMessage => {
      msg match {
        case StopMsg(_,_) => stop()
        case InitMsg(params: ActorPath) => init(params)
        case InitMsg(params) => masterPath = params.toString
        case _: HeartbeatMsg => sendHeartbeat()
        case msg: MasterSlaveProtocol => dispatchMonitorMsg(msg)
        case _ => log.info("received a message:" + msg.getClass)
      }
    }
    case xm => unhandled(xm)
  }

  def init(param: ActorPath){
    masterPath = param.toString
    sendHeartbeat()
  }

  def stop() {
    this.isStopped = true
    for (child <- context.children)
          context.stop(child)
    context.stop(self)
  }

  override def postStop(){
    super.postStop()
    log.info("actor stopped:" + self.path)
  }

}

object HeartBeatActor {
  def apply(master:ActorRef, isLinux:Boolean=false) = {
    new HeartBeatActor(master.path.toString, isLinux)
  }
}