package org.nicta.wdy.hdm.coordinator

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{Actor, ActorLogging}
import org.nicta.wdy.hdm.message.{AddHDMsMsg, JobCompleteMsg, RegisterPromiseMsg}
import org.nicta.wdy.hdm.model.HDM

import scala.concurrent.Promise

/**
 * Created by Tiantian on 2014/12/21.
 */
class ResultHandler extends Actor with ActorLogging{

  val promiseMap = new ConcurrentHashMap[String, Promise[HDM[_]]]()

  def appId(name:String, version:String) = s"$name#$version"


  @throws(classOf[Exception])
  override def preStart(): Unit = {
    super.preStart()
    log.info(s"Starting a ResultHandler actor...")
  }

  override def receive: Receive = {

    case AddHDMsMsg(appId, _, _ ) =>
      val promise = Promise[HDM[_]]()
      promiseMap.put(appId, promise)
      sender ! promise

    case RegisterPromiseMsg(appName, version, hdmID, _) =>
      val promise = Promise[HDM[_]]()
      promiseMap.put(hdmID, promise)
      log.info(s"Registered a promise with Id: $hdmID")
      sender ! promise

    case JobCompleteMsg(appId, state, result) =>
      log.info(s"Received a job complete msg with Id: $appId")
      val promise = promiseMap.get(appId)
      if(promise ne null){
        state match {
          case 0 => promise.success(result.asInstanceOf[HDM[_]])
          case other => promise.failure(new Exception("job failed because of: " + other))
        }
      } else log.error(s"Couldn't find matched promise with Id: $appId")

    case x => unhandled(x)
  }
}
