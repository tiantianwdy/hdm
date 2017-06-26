package org.hdm.core.server

import java.util.concurrent.{Semaphore, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.hdm.core.io.Path
import org.hdm.core.utils.{Logging, NotifyLock}

import scala.collection.mutable

/**
  * Created by tiantian on 23/05/17.
  */
class SingleResourceManager extends ClusterResourceManager[Int] with Logging {

  import scala.collection.JavaConversions._

  val childrenMap: java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]
  val childrenRWLock = new ReentrantReadWriteLock()

  val workingSize = new Semaphore(0)

  //lock obj for none empty
  val nonEmtpy = new NotifyLock


  def waitNonEmpty =  {
    if(workingSize.availablePermits() < 1){
      log.info("enter wait..")
      nonEmtpy.acquire()
      log.info("exit wait..")
    }
  }

  def notifyNonEmpty =  {
    log.info("enter notify all..")
    if(! nonEmtpy.isAvailable){
      nonEmtpy.release()
      log.info("Notify non-empty resource...")
    }
    log.info("exit notify all..")
  }


  override def init(): Unit = {
    childrenMap.clear()
  }

  override def getAllResources(): mutable.Map[String, Int] = {
    childrenMap.map(tup => tup._1 -> tup._2.get())
  }


  override def getResource(resId: String): (String, Int) = {
    (resId, childrenMap.get(resId).get())
  }

  override def removeResource(resId: String): Unit = {
    if(childrenMap.containsKey(resId)){
      childrenRWLock.readLock().lock()
      val permits = childrenMap.get(resId).get()
      childrenRWLock.readLock().unlock()
      workingSize.acquire(permits)
      childrenRWLock.writeLock().lock()
      childrenMap.remove(resId)
      childrenRWLock.writeLock().unlock()

    }
  }

  override def decResource(resId: String, value: Int): Unit = {
    if(childrenMap.containsKey(resId)){
      workingSize.acquire(value)
      childrenRWLock.writeLock().lock()
      val permits = childrenMap.get(resId).get()
      childrenMap.get(resId).set(permits - value)
      childrenRWLock.writeLock().unlock()
    }
  }

  override def addResource(resId: String, defaultVal: Int): Unit = {

    val oldValue = if(childrenMap.containsKey(resId))
      childrenMap.get(resId).get()
    else 0
    childrenRWLock.writeLock().lock()
    childrenMap.put(resId, new AtomicInteger(defaultVal))
    if(defaultVal > oldValue){
      workingSize.release(defaultVal - oldValue)
      if(workingSize.availablePermits() > 0) notifyNonEmpty
    }
    childrenRWLock.writeLock().unlock()
  }

  override def require(value: Int): Unit = {
    workingSize.acquire(value)
  }

  override def release(value: Int): Unit = {
    workingSize.release(value)
  }

  override def incResource(resId: String, value: Int): Unit = {
    if(childrenMap.containsKey(resId)){
      childrenRWLock.writeLock().lock()
      val permits = childrenMap.get(resId).get()
      childrenMap.get(resId).set(permits + value)
      workingSize.release(value)
      if(workingSize.availablePermits() > 0) notifyNonEmpty
      childrenRWLock.writeLock().unlock()
    }
  }


  override def hasAvailableResource(req: Int): Boolean = {
    getAvailableResources(req).size > 0
  }

  override def waitForNonEmpty(): Unit = {
    //      if(workingSize.availablePermits() < 1) {
    if(!hasAvailableResource(0)) {
      log.info("waiting for available resources")
      waitNonEmpty
    }
  }

  def getAvailableResources(req:Int) = {
    childrenMap.filter(t => t._2.get() > req).map(s => Seq.fill(s._2.get()){Path(s._1)}).flatten.toSeq
  }
}
