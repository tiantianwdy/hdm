package org.nicta.wdy.hdm.coordinator

import akka.actor.Actor
import akka.actor.Actor.Receive
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Promise
import org.nicta.wdy.hdm.message.{JobCompleteMsg, AddJobMsg}
import org.nicta.wdy.hdm.model.HDM

/**
 * Created by Tiantian on 2014/12/21.
 */
class ResultHandler extends Actor{

  val promiseMap = new ConcurrentHashMap[String, Promise[HDM[_,_]]]()

  override def receive: Receive = {

    case AddJobMsg(appId, _, _ ) =>
      val promise = Promise[HDM[_,_]]()
      promiseMap.put(appId, promise)
      sender ! promise
    case JobCompleteMsg(appId, state, result) =>
      val promise = promiseMap.get(appId)
      state match {
        case 1 => promise.success(result.asInstanceOf[HDM[_,_]])
        case other => promise.failure(new Exception("job failed because of: " + other))
      }
    case x => unhandled(x)
  }
}
