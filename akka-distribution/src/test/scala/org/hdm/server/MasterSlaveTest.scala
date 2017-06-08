package org.hdm.akka.server

import akka.actor.Address
import akka.actor.ActorSystem
import akka.actor.Props
import org.hdm.akka.actors.MasterActor
import com.typesafe.config.ConfigFactory
import org.hdm.akka.MyClusterActor

object MasterSlaveTest extends App {

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
		  		hostname = "172.18.0.1"
		  		port = 8998
		  	}
		  }  
      }
      """)
    val masterSystem = ActorSystem("masterSys", ConfigFactory.load(masterConf))
    val masterAddr = Address("akka", "masterSys", "172.18.0.1", 8998)
    val masterRooter = masterSystem.actorOf(Props[MyClusterActor], "master")
  }

  startMaster()

}



