package org.hdm.akka

import com.typesafe.config.ConfigFactory

trait TestConfig {

    val testMasterConf = ConfigFactory.parseString("""
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
		        hostname = "127.0.0.1"
		  		  port = "8999"
		    }
		  }
		}
      """)

  val testSlaveConf = ConfigFactory.parseString("""
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
		        hostname = "127.0.0.1"
    		   	port = "10010"
		    }
		  }
		}
      """)
      
}