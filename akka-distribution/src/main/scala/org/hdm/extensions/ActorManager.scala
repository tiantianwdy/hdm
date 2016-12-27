package org.hdm.akka.extensions

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.mapAsScalaConcurrentMap
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable

import org.hdm.akka.configuration.ActorConfig

import akka.actor.Actor
import akka.actor.ActorContext

/**
 * 管理各个Actor配置参数的manager，负责维护和更新各个actor的初始化所需参数和配置，可在运行时动态修改
 * @author wudongyao
 * @Date 2013-7-1
 * @version 0.0.1
 */
@deprecated("replaced by org.hdm.akka.extensions.ActorConfigExteion","0.0.1")
class ActorManager(var context: ActorContext) {

  /**
   * actorPath-> ActorConfig
   */
  val configurationMap = new ConcurrentHashMap[String, CopyOnWriteArrayList[ActorConfig]]()

  /**
   * cache the mapping from configuration type to configuration instance
   * add to map [class -> ActorConfig]
   */
  def addActorInstance(conf: ActorConfig) {
    configurationMap.getOrElseUpdate(conf.deploy.parentPath, {
      new CopyOnWriteArrayList[ActorConfig]()
    }).add(conf)
    println(s"current actors in actorManafer: ${configurationMap.mkString("\n")}")
  }

  def update(actorPath: String, params: Any) = {
    configurationMap.flatMap {
      case (k, lst) => {
        val findLst = lst.find(cf => cf.actorPath == actorPath)
        val newLst = findLst.map(_.withParams(params))
        lst --= findLst ++= newLst
      }
    }
  }
  /**
   *
   */
  def remove(confId: String) {
    configurationMap -= (confId)
  }

  /*  def createActor(queue:ConfType, actorPath:String, name:String):ActorRef*/

  def allTypes(): mutable.Set[String] = {
    asScalaSet(configurationMap.keySet())
  }

  /**
   * 获取parentPath 下所有children 的配置信息
   */
  def getChildren(parentPath: String): List[ActorConfig] = {
    configurationMap.get(parentPath) match {
      case null => List()
      case confList: CopyOnWriteArrayList[ActorConfig] => confList.toList
    }
  }

  /**
   * 获取actorPath对应的actor的配置信息
   */
  def getConfig(actorPath: String): Seq[ActorConfig] = {
    configurationMap.flatMap {
      case (k, v) => v.find(_.deploy.path == actorPath)
    }.toBuffer
  }

  def getAllConfig(clazzName: String): Seq[ActorConfig] = {
    configurationMap.flatMap {
      case (k, v) => v.filter(conf => conf.clazzName == clazzName)
    }.toBuffer
  }

  /**
   * java api
   */
  def getConfigJava(actorPath: String): java.util.List[ActorConfig] = getConfig(actorPath)

  def getChildrenJava(parentPath: String): java.util.List[ActorConfig] = {
    getChildren(parentPath)
  }
}