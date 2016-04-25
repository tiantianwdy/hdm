package org.nicta.wdy.hdm.coordinator

import akka.actor.{ActorLogging, Actor}
import akka.actor.Actor.Receive
import java.util.concurrent.ConcurrentHashMap
import org.nicta.wdy.hdm.server.DependencyManager

import scala.concurrent.Promise
import org.nicta.wdy.hdm.message.{RegisterPromiseMsg, JobCompleteMsg, AddHDMsMsg}
import org.nicta.wdy.hdm.model.ParHDM

/**
 * Created by Tiantian on 2014/12/21.
 */
class ResultHandler extends Actor with ActorLogging{

  val promiseMap = new ConcurrentHashMap[String, Promise[ParHDM[_,_]]]()

  def appId(name:String, version:String) = s"$name#$version"

  override def receive: Receive = {

    case AddHDMsMsg(appId, _, _ ) =>
      val promise = Promise[ParHDM[_,_]]()
      promiseMap.put(appId, promise)
      sender ! promise

    case RegisterPromiseMsg(appName, version, _) =>
      val promise = Promise[ParHDM[_,_]]()
      promiseMap.put(appId(appName, version), promise)
      sender ! promise

    case JobCompleteMsg(appId, state, result) =>
      val promise = promiseMap.get(appId)
      if(promise ne null){
        state match {
          case 1 => promise.success(result.asInstanceOf[ParHDM[_,_]])
          case other => promise.failure(new Exception("job failed because of: " + other))
        }
      } else log.error(s"Couldn't find matched promise with Id: $appId")

    case x => unhandled(x)
  }
}
