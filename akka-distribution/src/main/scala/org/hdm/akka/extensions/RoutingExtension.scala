package org.hdm.akka.extensions

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.mapAsScalaConcurrentMap

import org.hdm.akka.configuration.{ActorConfig, Deployment}
import org.hdm.akka.messages.RoutingProtocol
import org.hdm.akka.messages.RoutingQueryMsg
import org.hdm.akka.messages.RoutingRemove
import org.hdm.akka.messages.RoutingSyncMsg
import org.hdm.akka.messages.RoutingUpdateMsg
import org.hdm.akka.monitor.Monitable

import akka.actor._
import org.hdm.akka.configuration.ActorConfig
import annotation.tailrec
import org.hdm.akka.messages.RoutingQueryMsg
import scala.Some
import org.hdm.akka.messages.RoutingUpdateMsg
import org.hdm.akka.messages.RoutingRemove
import org.hdm.akka.messages.RoutingSyncMsg
import org.hdm.akka.configuration.ActorConfig
import org.hdm.akka.server.SmsSystem
import java.net.InetAddress

/**
 * 维护Actor的业务路径到实际部署路径的路由模块
 * 具有相同业务路径的Actor理论上具备同样业务功能的节点，只是部署的位置不同
 * 这个路由模块的目的是为了屏蔽cluster中相同功能节点的位置差异，方便业务上选择处理节点
 * @author wudongyao
 * @date 2013-7-14
 * @version 0.0.1
 *
 */
trait RoutingExtension extends Extension with Monitable {

  //  val akkaPrefix = """akka.\w+://"""
  //  val ipPattern = """\d+.\d+.\d+.\d+(:\d+)?"""
  val akkaPrefix = """akka(.\w+)?://"""
  val ipPattern = """@\d+.\d+.\d+.\d+(:\d+)?"""

  /**
   * 添加一个rooting,  映射 businessPath -> config
   * @param businessPath: String
   * @param config: ActorConfig
   */
  def addRooting(businessPath: String, config: ActorConfig): String

  /**
   * 添加一个rooting，businessPath = parseBusinessPath(actorPath: String)
   * @param config : ActorConfig
   */
  def addRooting(config: ActorConfig): String

  /**
   * 一次性添加一个ActorConfig 列表
   * @param confList : List[ActorConfig]
   * @return List[String] List of businessPath affected
   */
  def addRooting(confList: List[ActorConfig]): List[String]

  /**
   * 通过actorPath使用默认规则生成businessPath
   * @param actorPath: String
   */
  def parseBusinessPath(actorPath: String): String = {
    actorPath.replaceFirst(akkaPrefix, "").replaceFirst(ipPattern, "")
  }

  /**
   * 获取当前businessPath下的所有Actor信息
   * @param businessPath :String
   * @return
   */
  def getRooting(businessPath: String): List[ActorConfig]

  def getRooting(config: ActorConfig): List[ActorConfig]

  def getAllRooting: List[ActorConfig]

  def allBusinessPaths: List[String]

  def removeRooting(config: ActorConfig)

  /**
   * 删除原部署路径为actorPath的actor Rooting
   * @param actorPath : String
   */
  def removeRooting(actorPath: String)

  def removeAllRooting(businessPath: String): List[ActorConfig]

  def removeAllRooting()

  /**
   * 更新一个路径为actorPath的Actor的状态
   * @param actorPath : String
   * @param state : Int
   * @return  Option[List of ActorConfig]
   */
  def updateState(actorPath: String, state: Int): Option[List[ActorConfig]]

  def updateRooting(actorPath: String, conf: ActorConfig)

  def updateRooting(oldConf: ActorConfig, conf: ActorConfig)

  /**
   * 查找满足条件的ActorConfig
   * @param f  查找条件
   * @return 满足条件的  List[ActorConfig]
   */
  def find(f: ActorConfig => Boolean): List[ActorConfig]

  /**
   * 查找参数List中状态可用的ActorConfig
   * @param pathList List of business paths 使用逻辑路径
   * @return List[ActorConfig] 匹配到actor配置
   */
  def findAvailable(pathList: List[String]): List[ActorConfig]

}

/**
 * RoutingExtension 实现 负责维护 businessPath到ActorConfig的映射
 * 提供增删改查接口
 */
class RoutingExtensionImpl extends RoutingExtension {


  val routingMap = new ConcurrentHashMap[String, CopyOnWriteArrayList[ActorConfig]]()

  def addRooting(businessPath: String, config: ActorConfig): String = {
    routingMap.getOrElseUpdate(businessPath, {
      new CopyOnWriteArrayList[ActorConfig]()
    }).add(config)
    businessPath
  }

  def addRooting(config: ActorConfig): String = {
    if (config ne null)
      addRooting(parseBusinessPath(config.actorPath), config)
    else ""
  }

  def addRooting(confList: List[ActorConfig]): List[String] = confList.flatMap {
    Nil :+ addRooting(_)
  }

  def allBusinessPaths: List[String] = {
    routingMap.keySet().toList
  }

  def removeRooting(actorPath: String) {
    routingMap.flatMap {
      case (k, v) => v.map {
        cf => if (cf.actorPath == actorPath) v.remove(cf)
      }
    }
  }

  def removeRooting(config: ActorConfig) {
    removeRooting(config.actorPath)
  }

  def removeAllRooting(businessPath: String) = {
    val confList = routingMap.remove(businessPath)
    if (confList != null) confList.toList; else List()
  }

  def removeAllRooting() {
    routingMap.clear()
  }

  def getRooting(businessPath: String) = routingMap.get(businessPath) match {
    case confList: CopyOnWriteArrayList[ActorConfig] if (confList ne null) => confList.toList
    case _ => List[ActorConfig]()
  }

  def getRooting(config: ActorConfig) = {
    getRooting(parseBusinessPath(config.actorPath))
  }

  def getAllRooting: List[ActorConfig] = routingMap.flatMap {
    case (k, v) => v.toList
  }.toList

  def updateRooting(oldConf: ActorConfig, conf: ActorConfig) {
    removeRooting(oldConf)
    addRooting(conf)
  }

  def updateRooting(actorPath: String, conf: ActorConfig) {
    removeRooting(actorPath)
    addRooting(conf)
  }

  def updateState(actorPath: String, state: Int) = routingMap.get(parseBusinessPath(actorPath)) match {
    case null => None
    case confList: CopyOnWriteArrayList[ActorConfig] => {
      Some(confList.toList.filter(c => c.actorPath == actorPath).map {
        cf => {
          confList.add(cf.withState(state))
          confList.remove(cf)
          cf.withState(state)
        }
      })
    }
  }

  def find(f: ActorConfig => Boolean): List[ActorConfig] = {
    routingMap.flatMap {
      case (k, lst) => lst.filter(f)
    }.toList
  }

  /**
   * 查找pathList中可用的ActorConfig，优先查找非繁忙状态的
   * 按照逻辑地址进行匹配，与Actor物理地址无关
   * @param pathList List of business paths
   * @return List[ActorConfig]
   */
  def findAvailable(pathList: List[String]): List[ActorConfig] = {

    /**
     * 递归查找状态合适的actor
     * 查找优先级从高到低
     * @param logicPaths 逻辑路径
     * @param level 初始优先级
     * @param leastLevel 最低优先级
     * @return
     */
    @tailrec
    def findRecur(logicPaths: List[String], level: Int, leastLevel: Int): List[ActorConfig] = {
      //查找条件
      val condition = {
        conf: ActorConfig => logicPaths.contains(parseBusinessPath(conf.actorPath)) && conf.deploy.state == level
      }

      if (level < leastLevel) List[ActorConfig]()
      else find(condition) match {
        case actorList: List[ActorConfig] if (actorList.length > 0) => actorList
        case _ => findRecur(pathList, level - 1, leastLevel)
      }
    }

    val logicPathList = pathList.map {
      path => parseBusinessPath(path)
    }

    findRecur(logicPathList, Deployment.DEPLOYED_NORMAL, Deployment.UN_DEPLOYED)
  }

  /**
   * @todo to be implemented
   */
  def getMonitorData(reset: Boolean) = ???

}

object RoutingExtension {

  lazy val hostAddress =  InetAddress.getLocalHost.getHostAddress

  def globalSystemAddress(): Address = {
    val p = SmsSystem.systemPort getOrElse 2552
    val systemName = Option(SmsSystem.system) map {system => system.name} getOrElse "default"
    val h = RoutingExtension.hostAddress
    Address(protocol = "akka.tcp", system = systemName, host = h, port = p)
  }
}

/**
 * provider, 用于被Akka extension 调用创建对应的Extension实现
 */
object RoutingExtensionProvider
  extends ExtensionId[RoutingExtension]
  with ExtensionIdProvider {

  override def lookup = RoutingExtensionProvider

  override def createExtension(system: ExtendedActorSystem): RoutingExtension = new RoutingExtensionImpl

  override def get(system: ActorSystem): RoutingExtension = super.get(system)

}

/**
 * 用于给actor继承的trait
 */
trait RoutingService {
  this: Actor =>
  val routingService = RoutingExtensionProvider(context.system)

  def handleRootingMsg(msg: RoutingProtocol) {
    msg match {
      case RoutingSyncMsg(confList, false) => routingService.addRooting(confList)
      case RoutingSyncMsg(confList, true) =>
        routingService.removeAllRooting(); routingService.addRooting(confList)
      case RoutingQueryMsg(businessPath, 0) => sender ! RoutingSyncMsg(routingService.getRooting(businessPath))
      case RoutingQueryMsg(_, 1) => sender ! RoutingSyncMsg(routingService.getAllRooting, discardOld = true)
      case RoutingUpdateMsg(actorPath, conf) => routingService.updateRooting(actorPath, conf)
      case RoutingRemove(businessPath, -1) => routingService.removeAllRooting()
      case RoutingRemove(actorPath, 0) => routingService.removeRooting(actorPath)
      case RoutingRemove(_, 1) => routingService.removeAllRooting()
      case x => unhandled(x)
    }
  }
}

