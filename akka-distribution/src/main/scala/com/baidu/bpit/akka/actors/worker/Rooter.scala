package com.baidu.bpit.akka.actors.worker

import com.baidu.bpit.akka.configuration.ActorConfig
import com.baidu.bpit.akka.extensions.RoutingExtension
import scala.collection.JavaConversions._

/**
 * actor 消息分发的rooting逻辑实现
 *
 * @author wudongyao
 * @date 2013-7-24
 * @version since 0.0.1
 *
 */
trait Rooter {

  /**
   * 从ListPath中选出满足条件的ActorList
   */
  def rooting(pathList: List[String], rootingService: RoutingExtension): List[ActorConfig]

  /**
   * 从ListPath中选出满足条件的第一个Actor
   */
  def rootingFirst(pathList: List[String], rootingService: RoutingExtension): Option[ActorConfig] = rooting(pathList, rootingService) match {
    case lst: List[ActorConfig] if (!lst.isEmpty) => Some(lst.head)
    case _ => None
  }

  /**
   *
   * @param pathList
   * @param rootingService
   * @param wtMap
   * @return
   */
  def routeWithWeight(pathList: List[String], rootingService: RoutingExtension, wtMap: Map[ActorConfig, Int]): List[ActorConfig]

  /**
   *
   * @param pathList
   * @param rootingService
   * @param wtMap
   * @return
   */
  def routeFirstWithWeight(pathList: List[String], rootingService: RoutingExtension, wtMap: Map[ActorConfig, Int]): Option[ActorConfig] = routeWithWeight(pathList, rootingService, wtMap) match {
    case lst: List[ActorConfig] if (!lst.isEmpty) => Some(lst.head)
    case _ => None
  }


  /**
   * java api
   */
  def rootingFirst4J(pathList: java.util.List[String], rootingService: RoutingExtension): ActorConfig = rootingFirst(asScalaBuffer(pathList).toList, rootingService) match {
    case Some(actConf) => actConf
    case _ => null
  }


}

/**
 * BalancingRooter，优先从空闲的actor中选出满足条件的，具体实现逻辑通过@link rootingService.findAvailable实现
 * 使用BalancingRooter 需要继承一个DispatchPolicy，选出空闲的actor后通过DispatchPolicy得到目标Actor
 */
trait BalancingRooter extends Rooter {

  this: DispatchPolicy =>

  def rooting(pathList: List[String], rootingService: RoutingExtension): List[ActorConfig] = dispatch(rootingService.findAvailable(pathList))

  def routeWithWeight(pathList: List[String], rootingService: RoutingExtension, wtMap: Map[ActorConfig, Int]) = dispatchWithWeight(rootingService.findAvailable(pathList), wtMap)

}

/**
 * 随机分发rooter，通过BalancingRooter+RandomDispatch组合实现
 */
class RandomRooter extends BalancingRooter with RandomDispatch

/**
 * 轮询Rooter
 */
class RoundRobinRooter extends BalancingRooter with RoundRobinDispatch

/**
 * 按照权重分发的rooter，通过DynamicWeightedDispatch + BalancingRooter实现
 */
class WeightRooter extends DynamicWeightedDispatch with BalancingRooter {

  var weightMap = Map.empty[ActorConfig, Int]

  def withWeightMap(wtMap: Map[ActorConfig, Int]) = {
    weightMap = wtMap
    this
  }
}

/**
 * 优先级Rooter,选取优先级排序最高的actor
 */
class PriorityRooter extends PriorityDispatch with BalancingRooter {

  var priorityMap = Map.empty[ActorConfig, Int]

  def withWeightMap(wtMap: Map[ActorConfig, Int]) = {
    priorityMap = wtMap
    this
  }
}

