package com.baidu.bpit.akka.server

import akka.actor.Address
import akka.actor.ActorSystem
import akka.actor.Props
import com.baidu.bpit.akka.actors.MasterActor
import com.typesafe.config.ConfigFactory

class MasterSlaveTest {

  def startMaster() {
    val masterConf = ConfigFactory.parseString("""
      akka{
		  actor {
		  	provider = "akka.remote.RemoteActorRefProvider"
		  	serialize-messages = on
		      serializers {
		        java = "akka.serialization.JavaSerializer"
		        proto = "akka.remote.serialization.ProtobufSerializer"
		      }
		  	}
		  remote {
		  	enabled-transports = ["akka.remote.netty.tcp"]
		  	netty.tcp {
		  		hostname = "172.22.218.177"
		  		port = 10010
		  	}
		  }  
      }
      """)
    val masterSystem = ActorSystem("masterSys", ConfigFactory.load(masterConf))
    val masterAddr = Address("akka", "masterSys", "172.22.218.177", 15000)
    val masterRooter = masterSystem.actorOf(Props[MasterActor], "master")
  }
}



