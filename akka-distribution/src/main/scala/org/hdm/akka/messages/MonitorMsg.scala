package org.hdm.akka.messages

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.bufferAsJavaList

import org.hdm.akka.monitor.MonitorData

/**
 * 监控消息扩展
 * @param monitorData
 */
case class MonitorMsg( monitorData: java.util.List[MonitorData])  extends MasterSlaveProtocol{


  def +(msg:MonitorMsg) = {
    sum(msg)
  }

  /**
   * 添加一条监控数据
   * @param msg 收到的监控消息
   * @return 总的监控数据列表
   */
  def sum(msg:MonitorMsg) = {
    MonitorMsg(monitorData ++ msg.monitorData)
  }
}