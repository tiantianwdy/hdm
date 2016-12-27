package org.hdm.akka

import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Props
import org.hdm.akka.actors.SlaveActor
import akka.actor.ActorRef
import org.hdm.akka.messages.JoinMsg

class SlaveActorOne {
  val masterAddr = Address("akka", "masterSys", "172.22.218.167", 15000)

  def startUpSlave(port: Int) : ActorRef = {
    System.setProperty("akka.actor.provider", "akka.remote.RemoteActorRefProvider")
    System.setProperty("akka.actor.serialize-messages", "on")
    System.setProperty("akka.remote.transport", "akka.remote.netty.NettyRemoteTransport")
    System.setProperty("akka.remote.netty.hostname", "")
    System.setProperty("akka.remote.netty.port", port.toString)

    val slaveSystem = ActorSystem("slaveSys")
    val masterRooter = slaveSystem.actorFor("akka://masterSys@172.22.218.167:15000/user/master")
    slaveSystem.actorOf(Props(new SlaveActor(masterRooter.path.toString)), "slave")
  }

}

object SlaveActorOne extends App {
  
//  val masterSystem = ActorSystem("masterSys", ConfigFactory.load())
//  val remoteRooter = slaveSystem.actorOf(Props(new SlaveActor(masterRooter)).withRouter(
//  RemoteRouterConfig(BroadcastRouter(2), Seq(slaveAddr1, slaveAddr2))), "slave")
//  val slaveAddr1 = AddressFromURIString("akka://slaveSys@172.22.218.167:15010")
//  val slaveAddr2 = AddressFromURIString("akka://slaveSys@172.22.218.167:15011")
  val slave1 = new SlaveActorOne().startUpSlave(15010)
  slave1 ! JoinMsg()
  Thread.sleep(1000)
//  val slave2 = new SlaveActorOne().startUpSlave(15011)
//  slave2 ! MasterMessage(JOIN, new Date(), "test")

}