package org.hdm.akka.persistence

import org.hdm.akka.messages.MasterSlaveProtocol
import org.hdm.akka.messages.BasicMessage
import org.hdm.akka.monitor.MonitorData
import java.util.concurrent.{LinkedBlockingQueue, ConcurrentHashMap}
import scala.collection.mutable
import org.hdm.akka.messages.HeartbeatMsg
import scala.collection.JavaConversions._
import org.slf4j.LoggerFactory
import java.util.concurrent.locks.ReentrantLock
import java.util
import java.util.ArrayList
import util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

/**
 * 用于缓存监控数据的服务类
 * @author wudongyao
 * @date 2013-8-29
 * @version 0.0.1
 *
 */
class CachedKeyValueService(var maxBufferSize: Int = 512) extends PersistenceService {

  val logger = LoggerFactory.getLogger(classOf[CachedKeyValueService])

  val dataCache = new ConcurrentHashMap[String, mutable.Map[String, util.Queue[MonitorData]]]

  val lock: ReentrantLock = new ReentrantLock()

  def saveBasicMessage(msg: BasicMessage) {

  }

  def saveMasterMessage(msg: MasterSlaveProtocol) {
    msg match {
      case HeartbeatMsg(_, _, data) => {
        data.foreach { d => addToMap(d); logger.debug(s"add monitor data:${d.toString}") }
      }
      case _ => //ignore
    }

    def addToMap(data: MonitorData) {
      val buffer = dataCache.getOrElseUpdate(data.prop, new ConcurrentHashMap[String, util.Queue[MonitorData]]())
        .getOrElseUpdate(data.key, new LinkedBlockingQueue[MonitorData]())
      lock.lock()
      while (buffer.size - maxBufferSize >= 0)
        buffer.poll()
      buffer.offer(data)
      lock.unlock()
    }
  }

  def clear() {
    dataCache.clear()
  }

  def getData(prop: String): java.util.List[MonitorData] = {
    if (dataCache.containsKey(prop)) {
      dataCache(prop).flatMap { case (k, v) => v }.toList
    } else List()
  }

  def getData(prop: String, key: String): java.util.List[MonitorData] = {
    if (dataCache.containsKey(prop)) {
      dataCache(prop).getOrElse(key, new ArrayList[MonitorData]()).toList
    } else List()
  }

  def getData(prop: String, keys: java.util.List[String]): java.util.List[MonitorData] = {
    keys.flatMap(k => getData(prop, k))
  }

  def keys(prop: String): java.util.List[String] = {
    if (dataCache.containsKey(prop)) {
      dataCache(prop).keys.toList
    } else List.empty[String]
  }

  def props()={
    dataCache.keys().toList
  }
  
  def sum(prop: String, key: String):Double = {
    getData(prop, key).map{d=>d.value.toDouble}.sum
  }

}