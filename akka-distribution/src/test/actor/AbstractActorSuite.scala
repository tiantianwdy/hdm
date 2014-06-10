package com.baidu.bpit.akka.actor

import junit.framework.TestCase
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem

abstract class AbstractActorSuite extends TestCase{
  
   val testConf = ConfigFactory.parseString("""
      akka{
		  actor {
		  	provider = "akka.remote.RemoteActorRefProvider"
		  	serialize-messages = off
		      serializers {
		        java = "akka.serialization.JavaSerializer"
		        proto = "akka.remote.serialization.ProtobufSerializer"
		      }
		  	}
		  remote {
		  	enabled-transports = ["akka.remote.netty.tcp"]
		   	netty.tcp {
		   		hostname = ""
		   	}
		  }  
      }
      """)
  protected val actorSystem = ActorSystem("testSys", ConfigFactory.load(testConf))
  
  val systemConf = ConfigFactory.load()
}