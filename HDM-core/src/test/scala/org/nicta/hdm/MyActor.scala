package org.nicta.hdm

import akka.actor.Actor
import akka.event.Logging
import com.baidu.bpit.akka.actors.worker.WorkActor

class MyActor extends Actor{
  val log = Logging(context.system,this)
  val data = "12,23,34"
  val lstData = List(
    "12,23,34",
    "223,444,11",
    "1222,4,5,6,8886"
  )

	override def preStart() {
	  log.info("Actor:" + context.self)
	}
	
	def receive = {
	  case "askMsg" =>sender ! "return"
    case f:(String => Array[String] ) =>
      println(f);println(data.split(",").toList); println(f(data).toList)
    case f2:(List[String] => List[Any] ) =>
      println(f2); println(f2(lstData).toString())
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

