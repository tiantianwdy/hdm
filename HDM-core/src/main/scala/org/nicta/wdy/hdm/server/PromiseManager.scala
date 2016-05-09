package org.nicta.wdy.hdm.server

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Promise

/**
 * Created by tiantian on 24/08/15.
 */
trait PromiseManager {

  def addPromise(id:String, promise:Promise[_]):Promise[_]

  def getPromise(id:String):Promise[_]

  def removePromise(id:String):Promise[_]
  
  def createPromise[T](id:String):Promise[T] = {
    val promise = Promise[T]()
    addPromise(id, promise)
    promise
  }

}


class DefPromiseManager extends  PromiseManager{

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
}
