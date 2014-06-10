package com.baidu.bpit.akka.actors.worker.javaapi

import com.baidu.bpit.akka.monitor.MonitorDataExtension
import akka.actor.Props
import akka.actor.UntypedActor
import akka.event.Logging
import com.baidu.bpit.akka.extensions.RoutingExtensionProvider
import com.baidu.bpit.akka.extensions.WorkActorExtension
import akka.actor.Actor
import com.baidu.bpit.akka.messages.{StateMsg, MasterSlaveProtocol}

/**
 * WorkActor的javaAPI实现，主要考虑到java不能使用trait继承，需要使用一些reference代替
 * java api for @link WorkActor
 * @author wudongyao
 * @date 2013-7-11
 * @version 0.0.1
 *
 */
abstract class JWorkActor(params: Any) extends UntypedActor {

  protected val log = Logging(context.system, JWorkActor.this)

  /**
   * WorkActor 的代理类
   * 主要负责把WorkActor 处理完的消息引流出来在@method messageReceived 中进行处理
   */
  class WorkActorWrapper(params: Any) extends Actor with WorkActorExtension {

    def receive = {
    case msg: MasterSlaveProtocol => handleMasterSlaveMsg(msg)
    case msg: AnyRef if msg ne null => process(msg)
    case x => unhandled(x); log.warning("received a unhandled message.")
    }
    
    override def initParams(params: Any) = {
      initiated(params)
    }

    override def update(path:String, configId: String, param: Any, discardOld: Boolean, propagate :Boolean) {
      updateParams(configId, param, discardOld)
    }

    def process = {
      case msg: Any => messageReceived(msg)
    }

    /**
     * work 调用这个方法时会向parent转发状态改变的消息，让parent处理
     * @param state 状态
     * @param cause 原因
     */
    override def stateChanged(state: Int, cause: Any): Unit = {
      context.actorSelection(self.path.parent.parent).tell(StateMsg(state, cause), context.parent)
    }
  }

  protected val underlying = getContext().actorOf(Props(new WorkActorWrapper(params)), "underlying")

  /**
   * 使用同一context中的counting服务
   */
  protected val counting = MonitorDataExtension(context.system)

  protected val rootingService = RoutingExtensionProvider(context.system)

  /**
   * 所有消息需要先经过底层的WorkActor处理
   */
  def onReceive(msg: Any) {
    underlying.forward(msg)
  }

  def initiated(params: Any): Int

  def updateParams(confId:String, params: Any, discardOld: Boolean)

  def messageReceived(msg: Any)

}
