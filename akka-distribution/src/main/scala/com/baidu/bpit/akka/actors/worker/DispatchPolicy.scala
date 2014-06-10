package com.baidu.bpit.akka.actors.worker

import com.baidu.bpit.akka.configuration.ActorConfig
import java.util.concurrent.atomic.AtomicInteger
import scala.Predef._
import com.baidu.bpit.akka.configuration.ActorConfig
import scala.Some

/**
 * 消息分发策略实现
 * @author wudongyao
 * @Date 13-7-26 , 下午3:46
 * @version 0.0.1
 */
trait DispatchPolicy {

  /**
   * 按照策略从 List of actorConfig 中选举出后选列表
   * @param confList List[ActorConfig]
   * @return List of matched ActorConfig
   */
  def dispatch(confList: List[ActorConfig]): List[ActorConfig]

  /**
   * 提供权重Map进行actor分发
   * @param confList List[ActorConfig]
   * @param wtMap List of matched ActorConfig
   * @return
   */
  def dispatchWithWeight(confList: List[ActorConfig], wtMap: Map[ActorConfig, Int]): List[ActorConfig]

  def randomSeed(maxInt: Int): Int = {
    ((Math.random() * maxInt) % maxInt).toInt
  }
}

/**
 * 随机分发策略
 */
trait RandomDispatch extends DispatchPolicy {

  def dispatch(confList: List[ActorConfig]) = confList match {
    case confList: List[ActorConfig] if (!confList.isEmpty) => {
      val index = randomSeed(confList.length)
      List(confList(index))
    }
    case _ => List()
  }

  def dispatchWithWeight(confList: List[ActorConfig], wtMap: Map[ActorConfig, Int]) = dispatch(confList)
}

/**
 * 轮询分发策略
 */
trait RoundRobinDispatch extends DispatchPolicy {

  val lastIndex = new AtomicInteger(0)

  def dispatch(confList: List[ActorConfig]) = confList match {
    case confList: List[ActorConfig] if (!confList.isEmpty) => {
      val index = lastIndex.incrementAndGet % confList.length
      List(confList(index))
    }
    case _ => List()
  }

  def dispatchWithWeight(confList: List[ActorConfig], wtMap: Map[ActorConfig, Int]) = dispatch(confList)
}

/**
 * 按优先级分发策略
 */
trait PriorityDispatch extends DispatchPolicy {

  var priorityMap: Map[ActorConfig, Int]

  def dispatch(confList: List[ActorConfig]) = dispatchWithWeight(confList, priorityMap)

  def dispatchWithWeight(confList: List[ActorConfig], wtMap: Map[ActorConfig, Int]) = confList match {
    case confList: List[ActorConfig] if (!confList.isEmpty) => {
      val candidateMap = wtMap.filter {
        case (k, v) => confList.contains(k)
      }
      if (!candidateMap.isEmpty) {
        val indexSeq = candidateMap.toList.sortWith(_._2 > _._2).map(_._1)
        List(indexSeq.head)
      } else List()
    }
    case _ => List()
  }
}

/**
 * 动态权重分发策略（每次分发都重新计算权重映射）
 */
trait DynamicWeightedDispatch extends DispatchPolicy with WeightedMapper {

  var weightMap: Map[ActorConfig, Int]

  def dispatch(confList: List[ActorConfig]) = dispatchWithWeight(confList, weightMap)

  def dispatchWithWeight(confList: List[ActorConfig], wtMap: Map[ActorConfig, Int]) = confList match {
    case confList: List[ActorConfig] if (!confList.isEmpty) => {
      //生成权重区间
      val mappingInfo = weightedMapping(wtMap.filter {
        case (k, v) => confList.contains(k)
      })
      //权重区间映射
      mappingInfo._1.find(actorName => randomSeed(mappingInfo._3) < mappingInfo._2(actorName)) match {
        case Some(conf) => List(conf)
        case _ => List()
      }
    }
    case _ => List()
  }
}

trait WeightedMapper {
  /**
   * 通过配置生成权重Map
   * @return 三元组（排序后的配置表, 权重区间映射，最大权重值）
   */
  def weightedMapping(weightMap: Map[ActorConfig, Int]): (List[ActorConfig], Map[ActorConfig, Int], Int) = {
    val indexSeq = weightMap.toList.sortWith(_._2 > _._2).map(_._1)
    var sum = 0
    val weightedMapping = Map.empty[ActorConfig, Int] ++ indexSeq.map {
      conf => {
        sum += weightMap(conf)
        (conf -> sum)
      }
    }
    (indexSeq, weightedMapping, sum)
  }
}
