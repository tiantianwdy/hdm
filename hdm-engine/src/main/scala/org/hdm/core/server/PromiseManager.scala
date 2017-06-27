package org.hdm.core.server

import java.util.concurrent.ConcurrentHashMap

import org.hdm.core.utils.Logging

import scala.concurrent.Promise

/**
 * Created by tiantian on 24/08/15.
 */
trait PromiseManager {

  def addPromise(id:String, promise:Promise[_]):Promise[_]

  def getPromise(id:String):Promise[_]

  def removePromise(id:String):Promise[_]
  
  def createPromise[T](id:String):Promise[T]

}


class DefPromiseManager extends  PromiseManager with Logging {

  val promiseMap = new ConcurrentHashMap[String, Promise[_]]()

  override def addPromise(id: String, promise: Promise[_]): Promise[_] = {
    promiseMap.put(id,promise)
  }

  override def removePromise(id: String): Promise[_] = {
    promiseMap.remove(id)
  }

  override def getPromise(id: String): Promise[_] = {
    promiseMap.get(id)
  }

  override def createPromise[T](id:String):Promise[T]  = {
    if(promiseMap.containsKey(id)){
      getPromise(id).asInstanceOf[Promise[T]]
    } else {
      val promise = Promise[T]()
      log.info(s"created a new promise with id ${id}")
      addPromise(id, promise)
      promise
    }
  }
}
