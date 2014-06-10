package com.baidu.bpit.akka.actor

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import com.baidu.bpit.akka.actors.SlaveActor
import junit.framework.TestCase
import com.baidu.bpit.akka.configuration.ProxyConfig
import com.baidu.bpit.akka.messages.AddMsg
import com.baidu.bpit.akka.configuration.QueConfig
import com.baidu.bpit.akka.configuration.DispatchConfig

class SlaveActorTest extends TestCase{

  System.setProperty("akka.actor.provider", "akka.remote.RemoteActorRefProvider")
  System.setProperty("akka.actor.serialize-messages", "on")
  System.setProperty("akka.remote.transport", "akka.remote.netty.NettyRemoteTransport")
  System.setProperty("akka.remote.netty.hostname", "172.22.218.175")


  val slaveSystem = ActorSystem("slaveSys")

  def startUpSlave(name: String, port: Int, master: ActorRef): ActorRef = {
    System.setProperty("akka.remote.netty.port", port.toString)
    println(slaveSystem.actorOf(Props(new SlaveActor(master)), name).path.address)
    Thread.sleep(2000)
  }
  
  
  def testSlaveAddMsg(){
    val slave1 = startUpSlave("slave_15010", 15010, null)
//    val slave2 = startUpSlave("slave_15011", 15011, null)
    //add proxy
    val smppConf = ProxyConfig("SMPP")
    slave1 ! AddMsg(name="smppProxy", config=smppConf)
    //add queue
    val candidates:Seq[(String, Int, String)] = Seq(("INTP", 3, slave1.path.toString + "/smppProxy"))
    val queueConf = QueConfig(name="testQueue", queuePort=15010, candidateProxy=candidates)
    slave1 ! AddMsg(name="queue_15010", config=queueConf)
    //add dispatcher
    val dispatchConfig = new DispatchConfig()
    dispatchConfig.addMapping(15010, slave1.path.toString + "/queue_15010")
    slave1 ! AddMsg(name="dispatcher", config=dispatchConfig)
    slaveSystem.shutdown()
    Thread.sleep(10000)
  }
  
}