package com.baidu.bpit.akka.persistence

import com.baidu.bpit.akka.messages.BasicMessage
import com.baidu.bpit.akka.messages.MasterSlaveProtocol
import com.baidu.bpit.akka.messages.HeartbeatMsg
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import com.baidu.bpit.akka.monitor.MonitorData

/**
 * @author wudongyao
 * @date 2013-6-27
 * @version
 *
 */
class DefaultPersistenceService(var MaxDataLength: Int = 12) extends PersistenceService {

  val logger = LoggerFactory.getLogger(classOf[DefaultPersistenceService])

  val dataMap: mutable.Map[String, ListBuffer[MonitorData]] = mutable.HashMap()

  def saveBasicMessage(msg: BasicMessage) {

  }

  def saveMasterMessage(msg: MasterSlaveProtocol) {

    msg match {
      case HeartbeatMsg(_, _, data) => {
        data.foreach { d => addToMap(d); logger.debug(d.toString) }
      }
      case _ => //ignore
    }

    def addToMap(data: MonitorData) {
      val dataList = dataMap.getOrElse(data.prop, ListBuffer[MonitorData]())
      if (dataList.length >= MaxDataLength)
        dataList.remove(0, dataList.length - MaxDataLength + 1)
      dataList += data
      dataMap += (data.prop -> dataList)
      logger.debug(s"property: ${data.prop} current size: ${dataList.length}")
    }

  }

  def clear() {
    dataMap.clear()
  }

  def getData(propertyName: String) = {
    val dataList = dataMap.getOrElse(propertyName, ListBuffer[MonitorData]())
    dataList
  }

  def getData(propertyName: String, monitorId: String) = {
    val dataList = dataMap.getOrElse(propertyName, ListBuffer[MonitorData]()).filter(_.key == monitorId)
    dataList
  }

  def getData(propertyName: String, monitorIds: java.util.List[String]) = {
    val dataList = dataMap.getOrElse(propertyName, ListBuffer[MonitorData]()).filter(tuple => monitorIds.contains(tuple.key))
    dataList
  }

  def keys(propertyName: String) = {
    val dataList = for (data <- dataMap.getOrElse(propertyName, ListBuffer[MonitorData]())) yield data.key
    dataList.toList
  }

  def props()={
    dataMap.keys.toList
  }
  
  def sum(prop: String, key: String):Double = ???
}