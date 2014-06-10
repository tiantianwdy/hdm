package com.baidu.bpit.akka.actors.worker

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.junit.Test

import com.baidu.bpit.akka.configuration.ActorConfig
import com.baidu.bpit.akka.configuration.Deployment
import com.baidu.bpit.akka.extensions.RoutingExtensionImpl

class RooterTest {

  val conf = ActorConfig(id = "test/testMyActor", name = "testMyActor", clazzName = "com.baidu.bpit.akka.MyActor").withDeploy("akka.tcp://slaveSys@127.0.0.1:8080/user/smsSlave")
  val conf2 = ActorConfig(id = "test/testMyActor2", name = "testMyActor", clazzName = "com.baidu.bpit.akka.MyActor").withDeploy("akka.tcp://slaveSys@127.0.0.1:10010/user/smsSlave")
  val conf3 = ActorConfig(id = "test/testMyActor3", name = "testMyActor", clazzName = "com.baidu.bpit.akka.MyActor").withDeploy("akka.tcp://slaveSys@192.168.0.0:8999/user/smsSlave")
  val conf4 = conf.withState(Deployment.DEPLOYED_NORMAL).copy(id = "test/testMyActor4")
  val conf5 = conf2.withState(Deployment.DEPLOYED_NORMAL).copy(id = "test/testMyActor5")
  val conf6 = conf3.withState(Deployment.DEPLOYED_NORMAL).copy(id = "test/testMyActor6")
  val weightMap = Map(conf -> 3, conf2 -> 5, conf3 -> 7, conf4 -> 3, conf5 -> 6, conf6 -> 9)
  val rootingExtenstion = new RoutingExtensionImpl
  val pathList = List(conf.actorPath, conf2.actorPath).map(rootingExtenstion.parseBusinessPath(_))
  rootingExtenstion.addRooting(List(conf, conf2, conf3, conf4, conf5, conf6))
  val countMap = mutable.Map.empty[String, AtomicInteger]
  val total = 10000

  @Test
  def testWeightedMapping() {
    val mappingInfo = new WeightedMapper{}.weightedMapping(weightMap)
    println(mappingInfo._3.toString)
    println(mappingInfo._1.map { conf => conf.id })
    println(mappingInfo._2.toList.map { case (k, v) => (k.id, v) })
  }

  @Test
  def testWeightedRooter() {
    val rooter = new WeightRooter()
    for (i <- 1 to total) {
      rooter.routeFirstWithWeight(pathList, rootingExtenstion,weightMap) match {
        case Some(actorConf) => {
          countMap.getOrElseUpdate(actorConf.id, new AtomicInteger(0)).incrementAndGet()
        }
        case _ =>
      }
    }
    println(s"Weighted Mapping Prosibility:\n ${countMap.map { case (k, v) => k -> (v.get.toFloat / total) } mkString ("\n")}")
  }

  @Test
  def testRandomRooter() {
    val rooter = new RandomRooter {}
    for (i <- 1 to total) {
      rooter.rootingFirst(pathList, rootingExtenstion) match {
        case Some(actorConf) => {
          countMap.getOrElseUpdate(actorConf.id, new AtomicInteger(0)).incrementAndGet()
        }
        case _ =>
      }
    }
    println(s"Random Mapping Prosibility:\n ${countMap.map { case (k, v) => k -> (v.get().toFloat / total) } mkString ("\n")}")
  }
  
  @Test
  def testRoundRobinRooter() {
	  val rooter = new RoundRobinRooter {}
	  for (i <- 1 to total) {
		  rooter.rootingFirst(pathList, rootingExtenstion) match {
		  case Some(actorConf) => {
			  countMap.getOrElseUpdate(actorConf.id, new AtomicInteger(0)).incrementAndGet()
		  }
		  case _ =>
		  }
	  }
	  println(s"RoundRobin Mapping Statistc:\n ${countMap.map { case (k, v) => k -> (v.get()) } mkString ("\n")}")
  }
  
  @Test
  def testPriorityRooter() {
	  val rooter =  new PriorityRooter()
	  for (i <- 1 to total) {
		  rooter.routeFirstWithWeight(pathList, rootingExtenstion,weightMap)match {
		  case Some(actorConf) => {
			  countMap.getOrElseUpdate(actorConf.id, new AtomicInteger(0)).incrementAndGet()
		  }
		  case _ =>
		  }
	  }
	  println(s"Priority Mapping Prosibility:\n ${countMap.map { case (k, v) => k -> (v.get().toFloat / total) } mkString ("\n")}")
  }

}
