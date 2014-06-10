package com.baidu.bpit.akka.persistence

import com.baidu.bpit.akka.monitor.MonitorData
import com.baidu.bpit.akka.messages.HeartbeatMsg
import scala.collection.JavaConversions._
import org.junit.Test

class CachedKeyValueServiceTest {

  val persistenceService = new CachedKeyValueService(128)

  @Test
  def testGetMonitorData {
    val startTime = System.currentTimeMillis()
    for (i <- 1 to 12; j <- 0 to 100) {
      val data: List[MonitorData] = List(MonitorData(monitorName = "testMonitor", value = j.toString, key = i.toString, prop = "cpu", source = "local"))
      persistenceService.saveMasterMessage(HeartbeatMsg(null, null, data))
    }
    println(s"time eclipse: ${System.currentTimeMillis() - startTime} ms")
    println(persistenceService.getData("cpu").size())
    println(persistenceService.getData("cpu", "10").size())
    println(persistenceService.getData("cpu", List("11", "12")).size())
    println(persistenceService.getData("cpu", List("11", "12")).mkString("\n"))
    println(persistenceService.sum("cpu","11"))
  }
  
}