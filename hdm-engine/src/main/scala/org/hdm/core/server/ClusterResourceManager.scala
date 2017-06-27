package org.hdm.core.server

import java.util.concurrent.{CopyOnWriteArrayList, Semaphore, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicInteger

import org.hdm.core.scheduling.Scheduler

import scala.collection.mutable

/**
  * Created by tiantian on 23/05/17.
  */
trait ClusterResourceManager[T] {

  def init()

  def addResource(resId:String, defaultVal:T)

  def incResource(resId:String, value:T)

  def decResource(resId:String, value:T)

  def removeResource(resId:String)

  def getResource(resId:String):(String, T)

  def getAllResources():mutable.Map[String, T]

  def require(value:T)

  def release(value:T)

  def waitForNonEmpty()

  def hasAvailableResource(req:T):Boolean
}


class VectorResourceManager(vecLen:Int) extends ClusterResourceManager[Seq[Int]] {

  import scala.collection.JavaConversions._


  val resourceMap: java.util.Map[String, CopyOnWriteArrayList[Int]] = new ConcurrentHashMap[String, CopyOnWriteArrayList[Int]]

  val workingSize = new Semaphore(0)

  private def biop[U](v1:Seq[Int], v2:Seq[Int], func:(Int, Int) => U):Seq[U] = {
    v1.zip(v2).map(tup => func(tup._1, tup._2))
  }

  override def init(): Unit = {
    resourceMap.clear()
  }

  override def hasAvailableResource(req:Seq[Int]): Boolean = {
    resourceMap.exists{ res =>
      biop(res._2, req, (i1, i2) => i1 > i2).forall(t => t)
    }
  }

  override def getAllResources(): mutable.Map[String, Seq[Int]] = {
    resourceMap.map(tup => tup._1 -> tup._2.toIndexedSeq)
  }

  override def removeResource(resId: String): Unit = {
    resourceMap.remove(resId)
  }

  override def addResource(resId: String, defaultVal: Seq[Int]): Unit = {
    resourceMap.put(resId, new CopyOnWriteArrayList[Int](defaultVal))
  }

  override def decResource(resId: String, value: Seq[Int]): Unit = {
    if(resourceMap.containsKey(resId)){
      val oldValue = resourceMap.get(resId)
      val newValue = biop(oldValue, value, _ - _)
      resourceMap += resId -> new CopyOnWriteArrayList[Int](newValue)
    }
  }

  override def require(value: Seq[Int]): Unit = ???

  override def incResource(resId: String, value: Seq[Int]): Unit = {
    if(resourceMap.containsKey(resId)){
      val oldValue = resourceMap.get(resId)
      val newValue = biop(oldValue, value, _ + _)
      resourceMap += resId -> new CopyOnWriteArrayList[Int](newValue)
    } else {
      resourceMap += resId -> new CopyOnWriteArrayList[Int](value)
    }
  }

  override def getResource(resId: String): (String, Seq[Int]) = {
    resId -> resourceMap.get(resId)
  }

  override def release(value: Seq[Int]): Unit = ???

  override def waitForNonEmpty(): Unit = {
    while (hasAvailableResource(Seq.fill(vecLen){0})) {
      Thread.sleep(100)
    }
  }
}


