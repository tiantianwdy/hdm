package com.baidu.bpit.akka.persistence

import com.baidu.bpit.akka.messages.BasicMessage
import com.baidu.bpit.akka.monitor.MonitorData
import com.baidu.bpit.akka.messages.MasterSlaveProtocol
import akka.actor.{ActorSystem, ExtendedActorSystem, ExtensionIdProvider, ExtensionId}
import akka.actor.Extension

trait PersistenceService  extends Extension {

  def saveBasicMessage( msg: BasicMessage)
  
  def saveMasterMessage(msg:MasterSlaveProtocol)
  
  def clear()
  
  def getData(prop: String) : java.util.List[MonitorData]
  
  def getData(prop: String, key : String) : java.util.List[MonitorData]
  
  def getData(prop:String, keys:java.util.List[String]) : java.util.List[MonitorData]
  
  def keys(prop:String) : java.util.List[String]

  def props(): java.util.List[String]
  
  def sum(prop: String, key: String):Double
  
}


/**
 * provider, 用于被Akka extension 调用创建对应的Extension实现
 */
object PersistenceExtensionProvider
  extends ExtensionId[PersistenceService]
  with ExtensionIdProvider {

  override def lookup = PersistenceExtensionProvider

  override def createExtension(system: ExtendedActorSystem): PersistenceService = new CachedKeyValueService(512)

  override def get(system: ActorSystem): PersistenceService = super.get(system)

}

