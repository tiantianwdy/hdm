package org.nicta.wdy.hdm.server

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, Semaphore}

import org.nicta.wdy.hdm.scheduling.Scheduler

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by tiantian on 24/08/15.
 */
trait ResourceManager {

  def init()
  
  def addResource(resId:String, defaultVal:Int)

  def incResource(resId:String, value:Int)

  def decResource(resId:String, value:Int)

  def removeResource(resId:String)
  
  def getAllResources():mutable.Map[String, AtomicInteger]

  def require(value:Int)

  def release(value:Int)

  def waitForNonEmpty()


}

/**
 *
 */
class DefResourceManager extends ResourceManager{

  val followerMap: java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]

  val workingSize = new Semaphore(0)


  def init(): Unit = {
    followerMap.clear()
  }

  override def addResource(resId: String, defaultVal: Int): Unit = {
    val oldValue = if(followerMap.containsKey(resId)) followerMap.get(resId).get()
                   else 0
    followerMap.put(resId, new AtomicInteger(defaultVal))
    if(defaultVal > oldValue){
      workingSize.release(defaultVal - oldValue)
    }
  }

  override def removeResource(resId: String): Unit = {
    if(followerMap.containsKey(resId)){
      val value = followerMap.remove(resId).get()
      workingSize.acquire(value)
      //    workingSize.tryAcquire()
    }
  }

  override def decResource(resId: String, value: Int): Unit = {
    if(followerMap.containsKey(resId)){
      workingSize.acquire(value)
      if(value < 2)
        followerMap.get(resId).decrementAndGet()
      else {
        followerMap.get(resId).getAndAdd(0 - value)
      }
    }
  }

  override def incResource(resId: String, value: Int): Unit = {
    if(followerMap.containsKey(resId)){
      if(value < 2)
        followerMap.get(resId).incrementAndGet()
      else {
        followerMap.get(resId).getAndAdd(value)
      }
      workingSize.release(value)
    }
  }

  override def getAllResources(): mutable.Map[String, AtomicInteger] = {
    followerMap
  }

  override def require(value:Int) = {
    workingSize.acquire(value)
  }

  override def release(value:Int) = {
    workingSize.release(value)
  }

  override def waitForNonEmpty(): Unit = {
    while (Scheduler.getAllAvailableWorkers(getAllResources()).size <= 0) {
      Thread.sleep(100)
    }
  }
}
