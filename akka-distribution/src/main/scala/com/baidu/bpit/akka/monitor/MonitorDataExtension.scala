package com.baidu.bpit.akka.monitor

import scala.collection.JavaConversions.mapAsScalaConcurrentMap
import scala.collection.JavaConversions.seqAsJavaList

import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import com.baidu.bpit.akka.extensions.RoutingExtension

/**
 * actor监控数据模块实现，主要利用了Counting工具类
 * @author wudongyao
 * @date 2013-6-28
 * @version 0.0.1
 *
 */
class MonitorDataExtensionImpl extends Extension with Counting with Monitable {

  lazy val nodeAddress = RoutingExtension.globalSystemAddress().toString

  def getMonitorData(reset: Boolean = true) = {
    val dataSeq = for {
      (k, v) <- monitorDataMap
      (kk, ks) <- v
    } yield MonitorData(monitorName = getClass.toString, prop = k, key = kk, value = ks.toString, source = nodeAddress)
    if (reset) resetAll()
    dataSeq.toList
  }

}

/**
 * Akka的扩展接口实现
 * @author wudongyao
 * @date 2013-6-28
 * @version
 *
 */
object MonitorDataExtension
  extends ExtensionId[MonitorDataExtensionImpl]
  with ExtensionIdProvider {

  override def lookup = MonitorDataExtension

  override def createExtension(system: ExtendedActorSystem) = new MonitorDataExtensionImpl

  override def get(system: ActorSystem): MonitorDataExtensionImpl = super.get(system)

}