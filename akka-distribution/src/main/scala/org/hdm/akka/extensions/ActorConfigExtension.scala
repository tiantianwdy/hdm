package org.hdm.akka.extensions

import java.util.concurrent.ConcurrentHashMap
import akka.actor._
import org.hdm.akka.configuration.ActorConfig
import scala.collection.JavaConversions._

/**
 * 用与维护各个actor配置的模块,维护和更新各个actor的初始化所需参数和配置，并支持运行时动态修改
 * @author wudongyao
 * @date 2013-8-9
 * @version 0.0.1
 *
 */
trait ActorConfigExtension extends Extension{

  def addConf(path: String, conf: ActorConfig): String

  def addConf(conf: ActorConfig): String = addConf(conf.actorPath, conf)

  def getConf(path: String): ActorConfig

  def getConfList(path: List[String]): List[ActorConfig]

  def getSlaveConfList(slavePath: String, rootingService: RoutingExtension): List[ActorConfig]
  
  def update(path: String, conf: ActorConfig):ActorConfig
  
  def updateParam(path: String, params: Any):Option[ActorConfig]

  def find(f: ActorConfig => Boolean): Option[ActorConfig]
}

trait DefaultActorConfigExtension extends ActorConfigExtension {

  /**
   * 维护配置映射：
   * actorPath-> ActorConfig
   * 其中actorPath为actor的唯一绝对路径，对应一个唯一的actor部署实例
   */
  val configurationMap = new ConcurrentHashMap[String, ActorConfig]()

  def addConf(path: String, conf: ActorConfig): String = {
    configurationMap.put(path, conf)
    path
  }

  def getConf(path: String): ActorConfig = {
    configurationMap.get(path)
  }

  def getConfList(pathList: List[String]): List[ActorConfig] = {
    pathList.map { configurationMap.get(_) }
  }

  def getSlaveConfList(slavePath: String, rootingService: RoutingExtension): List[ActorConfig] = {
    getConfList(rootingService.find(conf => (conf.deploy ne null) && (conf.deploy.parentPath == slavePath)).map { _.actorPath })
  }

  def update(path: String, conf: ActorConfig):ActorConfig ={
    configurationMap.put(path, conf)
  }
  
  def updateParam(path: String, params: Any):Option[ActorConfig] ={
    configurationMap.getOrElse(path, None) match{
      case conf:ActorConfig => update(path, conf.withParams(params));Some( conf.withParams(params))
      case _ => None
    }
  }
  
  def find(condition: ActorConfig => Boolean) = {
    configurationMap.map{case(k,v) => v}.find(condition)
  }
}

/**
 * provider, 用于被Akka extension 调用创建对应的Extension实现
 */
object ActorConfigProvider
  extends ExtensionId[ActorConfigExtension]
  with ExtensionIdProvider {

  override def lookup = ActorConfigProvider

  override def createExtension(system: ExtendedActorSystem): ActorConfigExtension = new DefaultActorConfigExtension{}

  override def get(system: ActorSystem): ActorConfigExtension = super.get(system)

}