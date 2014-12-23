package org.nicta.wdy.hdm.coordinator

import scala.concurrent.Future

/**
 * Created by Tiantian on 2014/12/1.
 */
trait Coordinator {

  def join(entryPath:String)

  def leave()

  def vote(msg:Serializable)

  def declare(msg:Serializable)

  def broadcast(msg:Serializable)

  def request(path:String, msg:Serializable): Future[Serializable]

  def send(path:String, msg:Serializable):Unit

}

class AkkaCoordinator extends Coordinator {

  override def send(path:String, msg:Serializable): Unit = ???

  override def request(path:String, msg: Serializable): Future[Serializable] = ???

  override def broadcast(msg: Serializable): Unit = ???

  override def declare(msg: Serializable): Unit = ???

  override def vote(msg: Serializable): Unit = ???

  override def leave(): Unit = ???

  override def join(entryPath: String): Unit = ???
}
