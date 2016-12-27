package org.hdm.akka.actor

import akka.actor.Props
import org.hdm.akka.actors.worker.ProxyActor
import org.hdm.akka.configuration.ProxyConfig
import org.hdm.akka.configuration.SMPPConfig
import org.hdm.akka.configuration.QueConfig
import org.hdm.akka.actors.worker.QueActor
import org.hdm.akka.messages.InitMsg
import org.hdm.akka.messages.SmsMessage

class QueActorTest extends AbstractActorSuite{

  
    val smppConf = ProxyConfig("SMPP").asInstanceOf[SMPPConfig]
    smppConf.autoStart = false
    val proxy1 = actorSystem.actorOf(Props(ProxyActor(smppConf)), "smppProxy")
    val proxy2 = actorSystem.actorOf(Props(ProxyActor(smppConf)), "smppProxy2")
    val candidates:Seq[(String, Int, String)] = Seq(("INTP", 3, proxy1.path.toString),("INTP", 3, proxy2.path.toString))
    val queueConf = QueConfig(name="testQueue", queuePort=15000, candidateProxy=candidates)
    val queue =  actorSystem.actorOf(Props(QueActor(queueConf)), queueConf.name)
    

  
  def testDispatch(){
    val msg = SmsMessage(15000, "this is a test message", "008613522438071")
    queue ! InitMsg(queueConf)
    proxy1 ! InitMsg(smppConf)
    proxy2 ! InitMsg(smppConf)
    Thread.sleep(2000)
    queue ! msg
    //waiting for execution
    Thread.sleep(1000)
    actorSystem.shutdown()
    Thread.sleep(1000)
  }
  
  def testInit(){

    queue ! InitMsg(queueConf)
    Thread.sleep(10000)
  }
}