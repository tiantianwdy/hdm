package org.hdm.akka

import akka.actor.Actor
import akka.event.Logging
import akka.actor.ActorSystem
import akka.actor.Props
import org.hdm.akka.messages.CollectMsg
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Future
import akka.pattern.pipe
import scala.concurrent.{ ExecutionContext, Promise }
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import org.hdm.akka.actors.worker.WorkActor

class MyActor extends Actor{
    import context.dispatcher
	val log = Logging(context.system,this)
  val data = "12|23|34"
	 
	override def preStart() {
	  log.info("Actor:" + context.self)
	}
	
	def receive = {
	  case "askMsg" =>sender ! "return"
    case f:(String => Array[String] ) => println(f); println(f(data).mkString(","))
	  case str:String => {
		  log.info("received test String:" + str)
	  	}
//	  case obj: AnyRef => log.info("received a Object:" + obj.getClass)
	  case x =>unhandled(x);log.info("received a unhandled Object")
	}
	
	override def postStop() {
	  log.info("Actor:" + context.self + "has been stopped")
	}
}

class MyClusterActor extends WorkActor{
  
  def process = {
    case msg:Any => log.info(s"receive a $msg")
  }
}

