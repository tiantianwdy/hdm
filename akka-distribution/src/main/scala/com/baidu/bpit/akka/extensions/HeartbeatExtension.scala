package com.baidu.bpit.akka.extensions

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaConcurrentMap
import scala.collection.mutable
import scala.concurrent.duration.Duration
import com.baidu.bpit.akka.messages.HeartbeatMsg
import com.baidu.bpit.akka.messages.HeartbeatMsgResp
import akka.actor.{Extension, Actor, actorRef2Scala}
import akka.actor.ActorSelection.toScala
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * @author wudongyao
 * @date 13-8-29,下午5:14
 * @version
 */
trait HeartbeatExtension extends Extension {

  val nodeType: Int

  var heartbeatDelay = Duration.Zero

  var heartbeatInterval = Duration(15000, TimeUnit.MILLISECONDS)

  var hbTimeoutFactor = 3.0

  def heartbeatTicking(msg:HeartbeatMsg)

  def lastHeartbeatTime(name:String):Long

}

trait HeartbeatWatcher extends HeartbeatExtension {

  this: Actor =>

  val nodeType = HeartbeatNodeType.HEARTBEAT_WATCHER

  val watchList: mutable.Buffer[String] = new CopyOnWriteArrayList[String]()

  val heartbeatTimeVector: mutable.Map[String, Long] = new ConcurrentHashMap[String,Long]()

  def heartbeatReceived(watcher: String, msg: HeartbeatMsg) {
    heartbeatTimeVector += (watcher -> System.currentTimeMillis())
    if (!watchList.contains(watcher))
      watchList += watcher
    sender ! HeartbeatMsgResp(0)
  }

  def heartbeatTicking(msg:HeartbeatMsg) {
    val overTime = System.currentTimeMillis() - heartbeatInterval.toMillis * hbTimeoutFactor
    watchList.foreach {
      watchee  => if (heartbeatTimeVector.contains(watchee) && heartbeatTimeVector(watchee) < overTime) handleHeartBeatTimeout(watchee)
    }
  }

  def lastHeartbeatTime(name:String):Long = heartbeatTimeVector.getOrElse(name, -1)

  def handleHeartBeatTimeout(watchee: String)

}


trait HeartbeatWatchee extends HeartbeatExtension{

  this: Actor =>

  val nodeType = HeartbeatNodeType.HEARTBEAT_WATCHEE
  
  var watcherPath: String
  
  val lastHbTime = new AtomicLong(0);

  def heartbeatTicking(msg:HeartbeatMsg) {
      context.actorSelection(watcherPath) ! createHeartbeatMsg
      lastHbTime.set(System.currentTimeMillis())
  }
  
  def lastHeartbeatTime(name:String):Long = lastHbTime.get

  def createHeartbeatMsg: HeartbeatMsg = {
    HeartbeatMsg(self.path.toString,watcherPath,null)
  }

}

object HeartbeatExtension {

}

object HeartbeatNodeType {

  val HEARTBEAT_WATCHER = 0
  val HEARTBEAT_WATCHEE = 1
}
