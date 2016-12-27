package org.hdm.akka.extensions

import org.hdm.akka.configuration.ActorConfig
import org.hdm.akka.TestConfig
import akka.actor.ActorSystem
import org.junit.Before
import org.junit.Test
import scala.collection.mutable.ArrayBuffer

class ActorConfigExtensionTest extends TestConfig {

  var actorManager: ActorConfigExtension = null
  val rootingExtenstion = new RoutingExtensionImpl

  val conf = ActorConfig(id = "test/testMyActor", name = "testMyActor", clazzName = "org.hdm.akka.MyActor").withDeploy("akka.tcp://slaveSys@127.0.0.1:8999/user/smsSlave")
  val conf2 = ActorConfig(id = "test/testMyActor2", name = "testMyActor2", clazzName = "org.hdm.akka.MyActor").withDeploy("akka.tcp://slaveSys@127.0.0.1:10010/user/smsSlave")
  val conf3 = ActorConfig(id = "test/testMyActor3", name = "testMyActor3", clazzName = "org.hdm.akka.MyActor").withDeploy("akka.tcp://slaveSys@127.0.0.1:8999/user/smsSlave")

  @Before
  def beforeTest() {
    actorManager = new DefaultActorConfigExtension{}
  }

  @Test
  def testAddConf() {
    actorManager.addConf(conf)
    actorManager.addConf(conf2)
    actorManager.addConf(conf3)
    rootingExtenstion.addRooting(conf)
    rootingExtenstion.addRooting(conf2)
    rootingExtenstion.addRooting(conf3)
    println(actorManager.getSlaveConfList("akka.tcp://slaveSys@127.0.0.1:8999/user/smsSlave",rootingExtenstion))
    assert(actorManager.getSlaveConfList("akka.tcp://slaveSys@127.0.0.1:8999/user/smsSlave",rootingExtenstion).contains(conf))
    assert(actorManager.getSlaveConfList("akka.tcp://slaveSys@127.0.0.1:8999/user/smsSlave",rootingExtenstion).contains(conf3))
  }

  @Test
  def testGetAllConfig() {
    actorManager.addConf(conf)
    actorManager.addConf(conf2)
    actorManager.addConf(conf3)
    assert(actorManager.getConf(conf.actorPath)==conf)
    assert(actorManager.getConf(conf2.actorPath)==conf2)
    assert(actorManager.getConf(conf3.actorPath)==conf3)
  }

  @Test
  def testUpdateParams() {
    actorManager.addConf(conf.withParams("old"))
    actorManager.addConf(conf2.withParams("old2"))
    actorManager.addConf(conf3.withParams("old3"))
    actorManager.updateParam(conf.actorPath, "new")
    actorManager.updateParam(conf2.actorPath, "new2")
    actorManager.updateParam(conf3.actorPath, "new3")
    assert (actorManager.getConf(conf.actorPath).params =="new")
    assert (actorManager.getConf(conf2.actorPath).params =="new2")
    assert (actorManager.getConf(conf3.actorPath).params =="new3")
  }
}