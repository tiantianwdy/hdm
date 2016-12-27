package org.hdm.akka.extensions

import org.junit.{Before, Test}
import org.hdm.akka.{MyActor, TestConfig}
import akka.actor.{Props, ActorPath, ActorSystem}
import org.hdm.akka.configuration.ActorConfig
import org.hdm.akka.configuration.Deployment
import org.hdm.akka.server.SmsSystem

class RooterExtensionTest extends TestConfig {

  val rootingExtenstion = new RoutingExtensionImpl
  var actorSystem: ActorSystem = null

  val conf = ActorConfig(id = "test/testMyActor", name = "testMyActor", clazzName = "org.hdm.akka.MyActor").withDeploy("akka.tcp://slaveSys@127.0.0.1:8080/user/smsSlave")
  val conf2 = ActorConfig(id = "test/testMyActor2", name = "testMyActor", clazzName = "org.hdm.akka.MyActor").withDeploy("akka.tcp://slaveSys@127.0.0.1:10010/user/smsSlave")
  val conf3 = ActorConfig(id = "test/testMyActor3", name = "testMyActor", clazzName = "org.hdm.akka.MyActor").withDeploy("akka.tcp://slaveSys@192.168.0.0:8999/user/smsSlave")

  @Before
  def beforeTest() {
    actorSystem = ActorSystem("testSystem", testSlaveConf)
  }

  @Test
  def testGetGlobalAddress(){
    println(RoutingExtension.globalSystemAddress())
    val actor = actorSystem.actorOf(Props[MyActor],"cmpp_actor_guodu")
    actorSystem.shutdown()
    SmsSystem.startAsMaster(8999, false,testMasterConf)
    println(RoutingExtension.globalSystemAddress())
    val path = ActorPath.fromString( "akka.tcp://slaveSys/user/smsSlave/proxy/cmpp_actor_guodu")
    println(path.toStringWithAddress(RoutingExtension.globalSystemAddress()))
    println(actor.path.toStringWithAddress(RoutingExtension.globalSystemAddress()))
  }

  @Test
  def testParseActorPath() {
    println(rootingExtenstion.parseBusinessPath("akka.tcp://slaveSys@127.0.0.1:8080/user/smsSlave/proxy/cmpp_actor_guodu"))
    println(rootingExtenstion.parseBusinessPath("akka.tcp://slaveSys@127.0.0.1/user/smsSlave/proxy/cmpp_actor_guodu"))
    println(rootingExtenstion.parseBusinessPath("akka://slaveSys/user/smsSlave/sms-receive-actor"))
  }

  @Test
  def testAddRooting() {
    //    actorSystem.actorOf(Props[MyActor], "testMyActor")

    println("add rooting with different id:")
    rootingExtenstion.addRooting(conf.id, conf)
    rootingExtenstion.addRooting(conf2.id, conf)
    rootingExtenstion.addRooting(conf3.id, conf)
    rootingExtenstion.getRooting(conf.id).foreach(println)

    println("add rooting with different conf with the same businessPath:")
    rootingExtenstion.addRooting(conf)
    rootingExtenstion.addRooting(conf2)
    rootingExtenstion.addRooting(conf3)
    rootingExtenstion.getRooting(conf).foreach(println)
  }

  @Test
  def testAddActorList() {
    rootingExtenstion.addRooting(List(conf, conf2, conf3))
    assert(rootingExtenstion.getAllRooting == List(conf, conf2, conf3))
  }

  @Test
  def testUpdateState() {
    rootingExtenstion.addRooting(List(conf, conf2, conf3))
    println(rootingExtenstion.updateState(conf.actorPath, Deployment.DEPLOYED_NORMAL).mkString("\n"))
    assert(rootingExtenstion.getRooting(conf).map { c => c.deploy.state } == List(Deployment.UN_DEPLOYED, Deployment.UN_DEPLOYED, Deployment.DEPLOYED_NORMAL))
  }

  @Test
  def testUpdateConf() {
    rootingExtenstion.addRooting(List(conf, conf2, conf3))
    rootingExtenstion.getRooting(conf).foreach { c => println(c.deploy) }
    println(rootingExtenstion.updateRooting(conf, conf.copy(deploy = conf2.deploy)))
    rootingExtenstion.getRooting(conf).foreach { c => println(c.deploy) }
  }

  @Test
  def testGetAllRooting() {
    rootingExtenstion.addRooting(List(conf, conf2, conf3))
    assert(rootingExtenstion.getAllRooting == List(conf, conf2, conf3))
  }

  @Test
  def testRemoveAll() {
    rootingExtenstion.addRooting(List(conf, conf2, conf3))
    rootingExtenstion.removeAllRooting()
    assert(rootingExtenstion.getAllRooting == List())
  }

  @Test
  def testFind() {
    rootingExtenstion.addRooting(List(conf, conf2, conf3))
    assert(rootingExtenstion.find(conf => conf.name == "testMyActor") == List(conf, conf2, conf3))
    assert(rootingExtenstion.find(conf => conf.id == "testMyActor2") != List(conf2))
    assert(rootingExtenstion.find(conf => conf.deploy.state == Deployment.UN_DEPLOYED) == List(conf, conf2, conf3))
  }

  @Test
  def testFindAvailable() {
    rootingExtenstion.addRooting(List(conf, conf2, conf3))
    assert(rootingExtenstion.findAvailable(List(conf.id, conf2.id)) == List())
    val pathList = List(conf.actorPath, conf2.actorPath).map(rootingExtenstion.parseBusinessPath(_))
    val conf4 = conf.withState(Deployment.DEPLOYED_BUSY)
    rootingExtenstion.addRooting(conf4)
    assert(rootingExtenstion.findAvailable(pathList) == List(conf4))
    val conf5 = conf2.withState(Deployment.DEPLOYED_NORMAL)
    rootingExtenstion.addRooting(conf5)
    assert(rootingExtenstion.findAvailable(pathList) == List(conf5))
  }

  @Test
  def afterTest() {
    if (actorSystem != null)
      actorSystem.shutdown()
  }

}