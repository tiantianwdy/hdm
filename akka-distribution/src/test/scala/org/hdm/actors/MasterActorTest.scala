package org.hdm.akka.actors

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import org.hdm.akka.actors.worker.PersistenceActor
import org.hdm.akka.persistence.DefaultPersistenceService
import org.junit.Before
import org.junit.After
import akka.actor.ActorRef
import org.junit.Test
import org.hdm.akka.messages.AddMsg
import org.hdm.akka.configuration.Parameters
import org.hdm.akka.TestConfig

class MasterActorTest extends TestConfig{

  var actorSystem: ActorSystem = null

  @Before
  def beforeTest() {
    actorSystem = ActorSystem("testSys", testMasterConf)
  }

  @Test
  def testStartUp() {

    val persistenceActor = actorSystem.actorOf(Props(new PersistenceActor(new DefaultPersistenceService)), name = "persistenceActor")
    actorSystem.actorOf(Props(new MasterActor(persistenceActor)), "master")
  }

  @Test
  def testAddMsgToSlave {
    var master: ActorRef = null
    new Thread {
      override def run() {
        val persistenceActor = actorSystem.actorOf(Props(new PersistenceActor(new DefaultPersistenceService)), name = "persistenceActor")
        master = actorSystem.actorOf(Props(new MasterActor(persistenceActor)), "testMaster")
      }
    }.start
    Thread.sleep(1000)
    val slaveSys = ActorSystem("slaveSys", testSlaveConf)
    val slaveActor = slaveSys.actorOf(Props(new SlaveActor(master.path.toString)), "testSlave")
    val params = new Parameters {

    }
    Thread.sleep(2000)
    master ! AddMsg("testActor", slaveActor.path.toString(), "org.hdm.akka.MyActor", params)
    Thread.sleep(3000)

  }

  @After
  def afterTest() {
    actorSystem.shutdown
  }

}