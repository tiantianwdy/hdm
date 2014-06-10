package com.baidu.bpit.akka.monitor

import java.net.InetAddress

import scala.collection.JavaConversions.seqAsJavaList

import com.baidu.bpit.akka.messages.CollectMsg
import com.baidu.bpit.akka.messages.MonitorMsg

import akka.actor.Actor
import akka.actor.actorRef2Scala

/**
 * 系统监控实现，主要负责操作系统及JVM等系统资源的监控
 * @author wudongyao
 * @date 2013-6-27
 * @version
 *
 */
class SystemMonitor(val isLinux: Boolean, val systemId: String = "") extends Actor with Monitable {

  val address = InetAddress.getLocalHost
  val ip = address.getHostAddress

  def receive = {
    case CollectMsg => sender ! MonitorMsg(getMonitorData())
    case _ =>
  }

  def getMonitorData(reset:Boolean=false) = {
    val monitorId = ip + ":" + context.system.settings.config.getString("akka.remote.netty.tcp.port")
    //    val timestamp = System.currentTimeMillis()
    //    val jvmRate = SystemMonitorService.getJVMMemRate
    val jvmInfo = SystemMonitorService.getJVMMemInfo
    if (isLinux) {
      val cpuRate = SystemMonitorService.getCpuRate
      val memRate = SystemMonitorService.getMemRate
      val netRate = SystemMonitorService.getNetRate
      val data: java.util.List[MonitorData] = List(
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/cpu",
          key = s"$monitorId/cpuRate",
          value = cpuRate.toString,
          source = monitorId),
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/mem",
          key = s"$monitorId/memRate",
          value = memRate.toString,
          source = monitorId),
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/net",
          key = s"$monitorId/receive",
          value = netRate._1.toString,
          source = monitorId),
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/net",
          key = s"$monitorId/transmit",
          value = netRate._2.toString,
          source = monitorId),
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/jvm",
          key = s"$monitorId/memFree",
          value = jvmInfo(0).toString,
          source = monitorId),
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/jvm",
          key = s"$monitorId/memMax",
          value = jvmInfo(1).toString,
          source = monitorId),
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/jvm",
          key = s"$monitorId/memTotal",
          value = jvmInfo(2).toString,
          source = monitorId))
      data
      /*
          Seq((timestamp.toString, cpuRate.toString, monitorId, "cpu/cpuRate"), 
          (timestamp.toString, memRate.toString, monitorId, "mem/memRate"), 
          (timestamp.toString, netRate._1.toString, monitorId, "net/receive"),
          (timestamp.toString, netRate._2.toString, monitorId, "net/transmit"),
          (timestamp.toString, jvmInfo(0).toString, monitorId, "jvm/memFree"),
          (timestamp.toString, jvmInfo(1).toString, monitorId, "jvm/memTotal"),
          (timestamp.toString,  jvmInfo(2).toString, monitorId, "jvm/memMax"))
          */
    } else {
      /* 
        Seq((timestamp.toString, 0.toString, monitorId, "cpu/cpuRate"),
        (timestamp.toString, 0.toString, monitorId, "mem/memRate"),
        (timestamp.toString, 0.toString, monitorId, "net/receive"),
        (timestamp.toString, 0.toString, monitorId, "net/tansmit"),
        (timestamp.toString, jvmInfo(0).toString, monitorId, "jvm/memFree"),
        (timestamp.toString, jvmInfo(1).toString, monitorId, "jvm/memTotal"),
        (timestamp.toString, jvmInfo(2).toString, monitorId, "jvm/memMax"))
        */
      val data: java.util.List[MonitorData] = List(
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/cpu",
          key = s"$monitorId/cpuRate",
          value = 0.toString,
          source = monitorId),
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/mem",
          key = s"$monitorId/memRate",
          value = 0.toString,
          source = monitorId),
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/net",
          key = s"$monitorId/receive",
          value = 0.toString,
          source = monitorId),
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/net",
          key = s"$monitorId/transmit",
          value = 0.toString,
          source = monitorId),
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/jvm",
          key = s"$monitorId/memFree",
          value = jvmInfo(0).toString,
          source = monitorId),
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/jvm",
          key = s"$monitorId/memMax",
          value = jvmInfo(1).toString,
          source = monitorId),
        MonitorData(
          monitorName = "systemMonitor",
          prop = "system/jvm",
          key = s"$monitorId/memTotal",
          value = jvmInfo(2).toString,
          source = monitorId))
      data
    }
  }
}