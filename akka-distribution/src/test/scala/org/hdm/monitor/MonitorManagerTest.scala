package org.hdm.akka.monitor

import scala.collection.JavaConversions.seqAsJavaList
import org.hdm.akka.actors.HeartBeatActor
import org.hdm.akka.messages.CollectMsg
import org.hdm.akka.messages.JoinMsg
import org.hdm.akka.messages.MonitorMsg
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.testkit.TestActorRef
import junit.framework.TestCase
import org.junit.Test
import org.junit.After

class MonitorManagerTest {

  implicit val system = ActorSystem()

  @Test
  def testCollectData() {
    val hbActor = TestActorRef(new HeartBeatActor(null, false), "heartbeatMessage")
    //    val sysMonitor = system.actorOf(Props(new SystemMonitor(false)), "systemMonitor")
    val instance1 = system.actorOf(Props(new TestMonitor("instance_1", 1)))
    val instance2 = system.actorOf(Props(new TestMonitor("instance_2", 2)))
    val instance3 = system.actorOf(Props(new TestMonitor("instance_3", 3)))
    hbActor ! JoinMsg(slavePath = instance1.path.toString)
    hbActor ! JoinMsg(slavePath = instance2.path.toString)
    hbActor ! JoinMsg(slavePath = instance3.path.toString)
    hbActor ! CollectMsg
    for (i <- 1 to 10) {
      Thread.sleep(1000)
      hbActor ! CollectMsg
    }
  }

  @After
  def afterAll() {
    system.shutdown()
  }

}

class MockHeartbeatMonitor extends MonitorManager {

  def receive = dispatchMonitorMsg

}

class TestMonitor(val name: String, val monitorId: Int) extends Actor with Monitable {

  def receive = {
    case CollectMsg => sender ! MonitorMsg(getMonitorData())
  }

  def getMonitorData(reset:Boolean) = {
    val topic_medium_produce = MonitorData(monitorName = getClass().toString, value = (Math.random() * 100).toString, key = "emp-dispatcher-medium/produce", prop = "emp/topic", source = self.path.toString)
    val topic_medium_consume = MonitorData(monitorName = getClass().toString, value = (Math.random() * 100).toString, key = "emp-dispatcher-medium/consume", prop = "emp/topic", source = self.path.toString)
    val topic_guodu_produce = MonitorData(getClass().toString, value = (Math.random() * 100).toString, key = "emp-proxy-guodu_cmpp/produce", prop = "emp/topic", source = self.path.toString)
    val topic_guodu_consume = MonitorData(getClass().toString, value = (Math.random() * 100).toString, key = "emp-proxy-guodu_cmpp/consume", prop = "emp/topic", source = self.path.toString)

    val sms_frontend_consume = MonitorData(getClass().toString, value = (Math.random() * 100).toString, key = s"$monitorId/consume", prop = "emp/sms/frontend", source = self.path.toString)
    val sms_frontend_produce = MonitorData(getClass().toString, value = (Math.random() * 100).toString, key = s"$monitorId/produce", prop = "emp/sms/frontend", source = self.path.toString)
   
    val sms_dispatcher_consume = MonitorData(getClass().toString, value = (Math.random() * 100).toString, key = s"$monitorId/consume", prop = "emp/sms/dispatcher", source = self.path.toString)
    val sms_dispatcher_produce = MonitorData(getClass().toString, value = (Math.random() * 100).toString, key = s"$monitorId/produce", prop = "emp/sms/dispatcher", source = self.path.toString)
    List(topic_medium_produce, topic_medium_consume, topic_guodu_produce, topic_guodu_consume, sms_frontend_consume, sms_frontend_produce)
  }
}

