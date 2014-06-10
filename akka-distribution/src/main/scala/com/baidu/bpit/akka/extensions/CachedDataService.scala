package com.baidu.bpit.akka.extensions

import com.baidu.bpit.akka.server.SmsSystem
import akka.actor._
import scala.Some
import com.baidu.bpit.akka.messages.{Query, Reply, QueryProtocol, QueryExtension}

/**
 *
 * @author wudongyao
 * @date 14-1-3,下午1:32
 * @version
 */
trait CachedDataService {

  /**
   * 返回所有监控属性
   * @return
   */
  def props(): Array[Object]

  /**
   * 模糊匹配监控属性
   * @param prop
   * @return
   */
  def propsLike(prop: String): Array[Object]

  /**
   * 获取监控属性下的所有键值
   * @param prop
   * @return
   */
  def keys(prop: String): Array[Object]

  /**
   * 找到模糊匹配到prop,和key的键值参数
   * 多个prop匹配到时返回第一个prop，匹配到的key list
   * @param prop 匹配参数
   * @param key 匹配键值
   * @return   (prop,List（key）)
   */
  def liken(prop: String, key: String): (String, java.util.List[String])

  /**
   * 监控数据模糊查询
   * @param prop
   * @param key
   * @return
   */
  def dataLike(prop: String, key: String): Array[Array[Any]]

  /**
   * 求一个参数在缓存队列中的总累计值
   * @param prop
   * @param key
   * @return sum of (prop,key)
   */
  def sum(prop: String, key: String): Double

  /**
   * 求一个参数在时间周期内的总累计值
   * @param prop 参数名
   * @param key 参数键值
   * @param duration 统计的时间轴长度，会计算time < currentTime - duration 的统计值
   * @return
   */
  def sum(prop: String, key: String, duration: Long): Double

  /**
   * 计算监控属性简单表达式的当前值
   * @param lProp
   * @param lKey
   * @param op
   * @param rProp
   * @param rKey
   * @return  current value of property
   */
  def computeVal(lProp: String,
                 lKey: String,
                 op: String,
                 rProp: String = "",
                 rKey: String = "",
                 duration: Long = 600 * 1000): Double

}


class CachedDataServiceImpl extends CachedDataService {

  import scala.collection.JavaConversions._

  def props() = {
    SmsSystem.persistenceService.props().toArray
    //    Array.empty[String]
  }

  def propsLike(prop: String) = {
    SmsSystem.persistenceService.props().filter(_.containsSlice(prop)).toArray
  }

  def keys(prop: String) = {
    SmsSystem.persistenceService.keys(prop).toArray
  }


  def dataLike(prop: String, key: String): Array[Array[Any]] = {
    val likens = SmsSystem.persistenceService.props().filter(_.containsSlice(prop))
    likens.find(prop => SmsSystem.persistenceService.keys(prop).exists(_.containsSlice(key))) match {
      //find first matched prop
      case Some(p) =>
        SmsSystem.persistenceService.keys(p).find(_.containsSlice(key)) match {
          //find first matched key
          case Some(k) => getDataAsArray(p, k)
          case _ => Array.empty[Array[Any]]
        }
      case None => Array.empty[Array[Any]]
    }
  }

  def liken(prop: String, key: String) = {
    SmsSystem.persistenceService.props().find {
      p =>
        p.containsSlice(prop) && SmsSystem.persistenceService.keys(p).exists(_.containsSlice(key))
    } match {
      case Some(lp) => {
        val lk = SmsSystem.persistenceService.keys(lp).filter(_.containsSlice(key)).toList
        (lp, lk)
      }
      case _ => (prop, List())
    }
  }

  def sum(prop: String, key: String) = {
    SmsSystem.persistenceService.getData(prop, key).map {
      d => d.value.toDouble
    }.sum
  }

  /**
   *
   * @param prop
   * @param key
   * @param duration
   * @return
   */
  def sum(prop: String, key: String, duration: Long): Double = {
    val deadLine = if (duration > 0) System.currentTimeMillis() - duration; else System.currentTimeMillis() - 24 * 3600 * 1000 //default 1 day
    SmsSystem.persistenceService.getData(prop, key).filter(_.time > deadLine).map(_.value.toDouble).sum
  }

  /**
   * 计算监控属性的简单表达式值，目前支持加减乘除的一至二元操作
   * @param lProp
   * @param lKey
   * @param op
   * @param rProp
   * @param rKey
   * @param duration
   * @return  current value of property
   */
  def computeVal(lProp: String,
                 lKey: String,
                 op: String,
                 rProp: String = "",
                 rKey: String = "",
                 duration: Long = 600 * 1000): Double = {
    val lVal = if (duration > 0) sum(lProp, lKey, duration) else sum(lProp, lKey)
    if (op != null && !rProp.isEmpty && !rKey.isEmpty) {
      val rVal = if (duration > 0) sum(rProp, rKey, duration); else sum(rProp, rKey)
      op match {
        case "plus" | "+" => lVal + rVal
        case "minus" | "-" => lVal - rVal
        case "divide" | "/" => lVal / rVal
        case "multiply" | "*" => lVal * rVal
        case _ => lVal
      }
    } else lVal
  }

  def getDataAsArray(prop: String, key: String): Array[Array[Any]] = {
    val data = SmsSystem.persistenceService.getData(prop, key)
    data.map {
      data => Array[Any](data.time, data.value.toFloat)
    }.toArray
  }

}

/**
 * 缓存服务单例工厂
 */
object CachedDataService {

  lazy val cachedDataService = new CachedDataServiceImpl

  def apply() = cachedDataService
}

/**
 * 数据查询接口服务
 */
class CachedDataQueryService() extends QueryExtension {

  val cachedDataService = CachedDataService()

  def handleQueryMsg(msg: QueryProtocol): Option[Reply] = msg match {
    case Query(op, prop, key, duration, source, target) => {
      val res = try {
        op match {
          case "dataService/props" => cachedDataService.props()
          case "dataService/propsLike" => cachedDataService.propsLike(prop)
          case "dataService/keys" => cachedDataService.keys(prop)
          case "dataService/liken" => cachedDataService.liken(prop, key)
          case "dataService/dataLike" => cachedDataService.dataLike(prop, key)
          case "dataService/getData" => cachedDataService.getDataAsArray(prop, key)
          case "dataService/sum" => cachedDataService.sum(prop, key, duration)
          case _ => None
        }
      } catch {
        case _: Throwable => None
      }
      Some(Reply(result = res, op, prop, key, target, source))
    }
    case _ => None
  }
}


/**
 * akka extension 封装
 */
trait CachedDataExtension extends Extension with CachedDataService

class CachedDataExtensionImpl extends CachedDataServiceImpl with CachedDataExtension


/**
 * provider, 用于被Akka extension 调用创建对应的Extension实现
 */
object CachedDataExtensionProvider
  extends ExtensionId[CachedDataExtension]
  with ExtensionIdProvider {

  override def lookup = CachedDataExtensionProvider

  override def createExtension(system: ExtendedActorSystem): CachedDataExtension = new CachedDataExtensionImpl

  override def get(system: ActorSystem): CachedDataExtension = super.get(system)

}



