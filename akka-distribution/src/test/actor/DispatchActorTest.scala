package org.hdm.akka.actor

import org.hdm.akka.configuration.DispatchConfig
import org.hdm.akka.messages.{StopMsg, SmsMessage, InitMsg}
import akka.actor.Props
import org.hdm.akka.actors.worker.ProxyActor
import org.hdm.akka.configuration.ProxyConfig
import org.hdm.akka.actors.worker.DispatchActor
import org.hdm.akka.configuration.QueConfig
import org.hdm.akka.actors.worker.QueActor

class DispatchActorTest extends AbstractActorSuite {

  def testDispatchActor() {
    val smppConf = ProxyConfig("SMPP")
    smppConf.autoStart = false
    val proxy = actorSystem.actorOf(Props(ProxyActor(smppConf)), "smppProxy")
    val msg = SmsMessage(15000, "this is a test message", "008618612030915")
    
    val candidates:Seq[(String, Int, String)] = Seq(("INTP", 3, proxy.path.toString))
    val queueConf = QueConfig(name="queue", queuePort=15000, candidateProxy=candidates)
    val queue =  actorSystem.actorOf(Props(QueActor(queueConf)), queueConf.name)
    
    val dispatchConfig = new DispatchConfig()
    dispatchConfig.addMapping(15000, queue.path.toString)
    val dispatcher = actorSystem.actorOf(Props(DispatchActor(dispatchConfig)), "defaultDispatcher")
    proxy ! InitMsg(smppConf)
    queue ! InitMsg(queueConf)
    dispatcher ! InitMsg(dispatchConfig)
    Thread.sleep(2000)
    dispatcher ! msg

     Thread.sleep(3000)
     proxy ! StopMsg
    actorSystem.shutdown()
    Thread.sleep(3000)
  }

}