package com.baidu.bpit.akka.test.actor

import akka.actor.ActorSystem
import akka.actor.Props
import com.baidu.bpit.akka.actors.MasterActor
import junit.framework.TestCase
import akka.actor.ActorRef
import com.baidu.bpit.akka.actors.SlaveActor
import com.baidu.bpit.akka.messages.JoinMsg
import com.baidu.bpit.akka.configuration.ProxyConfig
import com.baidu.bpit.akka.messages.AddMsg
import com.baidu.bpit.akka.configuration.QueConfig
import com.baidu.bpit.akka.configuration.DispatchConfig

class MasterActorTest {

   
    
    
  //need to start a slave first
  //  val slave1 = masterSystem.actorFor("akka://slaveSys@172.22.218.167:15010/user/slave_15010")

  def testAddMsg() {
    System.setProperty("akka.actor.provider", "akka.remote.RemoteActorRefProvider")
    System.setProperty("akka.actor.serialize-messages", "on")
    System.setProperty("akka.remote.transport", "akka.remote.netty.NettyRemoteTransport")
    System.setProperty("akka.remote.netty.hostname", "172.22.218.167")
    System.setProperty("akka.remote.netty.port", "15100")
    val masterSystem = ActorSystem("masterSys")
    masterSystem.actorOf((Props[MasterActor]), "master")
    val master = masterSystem.actorFor("akka://masterSys@172.22.218.167:15000/user/master")
    val slavePath = "akka://slaveSys@172.22.218.167:15010/user/slave_15010"
//    slave1 ! JoinMsg( master.path,slave1.path)
    Thread.sleep(10000)
    //add proxy
    val smppConf = ProxyConfig("SMPP")
    master ! AddMsg(name="smppProxy", path=slavePath, config=smppConf)
    //add queue
    val candidates:Seq[(String, Int, String)] = Seq(("INTP", 3, slavePath + "/smppProxy"))
    val queueConf = QueConfig(name="testQueue", queuePort=15010, candidateProxy=candidates)
    master ! AddMsg(name="queue_15010",  path=slavePath, config=queueConf)
    //add dispatcher
    val dispatchConfig = new DispatchConfig
    dispatchConfig.addMapping(15010, slavePath + "/queue_15010")
    master ! AddMsg(name="dispatcher",  path=slavePath, config=dispatchConfig)
    masterSystem.shutdown()
    Thread.sleep(10000)
  }

    
}