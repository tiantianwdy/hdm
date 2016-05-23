package org.nicta.wdy.hdm.server

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{Semaphore, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicInteger

import org.nicta.wdy.hdm.utils.NotifyLock

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
 * Created by tiantian on 17/05/16.
 */
class MultiClusterResourceManager extends ResourceManager{

  val childrenMap: java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]

  val siblingMap: java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]


  val childrenWorkingSize = new Semaphore(0)

  val siblingWorkingSize = new Semaphore(0)

  //lock obj for none empty of all resources
  val nonEmtpy = new NotifyLock

  val childrenNonEmpty = new NotifyLock

  val siblingNonEmpty = new NotifyLock

  val childrenRWLock = new ReentrantReadWriteLock()
  val siblingRWLock = new ReentrantReadWriteLock()

  override def init(): Unit = {
    childrenMap.clear()
    siblingMap.clear()
  }

  override def getAllResources(): mutable.Map[String, AtomicInteger] = {
    childrenRWLock.readLock().lock()
    val res = childrenMap ++ siblingMap
    childrenRWLock.readLock().unlock()
    res
  }

  override def removeResource(resId: String): Unit = {
    if(childrenMap.containsKey(resId)){
      childrenRWLock.writeLock().lock()
      val permits = childrenMap.remove(resId).get()
      childrenRWLock.writeLock().unlock()
      childrenWorkingSize.acquire(permits)
    } else if (siblingMap.containsKey(resId)){
      siblingRWLock.writeLock()lock()
      val permits = siblingMap.remove(resId).get()
      siblingRWLock.writeLock().unlock()
      siblingWorkingSize.acquire(permits)
    }
  }

  override def decResource(resId: String, value: Int): Unit = {
    if(childrenMap.containsKey(resId)){
      childrenWorkingSize.acquire(value)
      childrenRWLock.writeLock().lock()
      childrenMap.get(resId).getAndAdd(-value)
      childrenRWLock.writeLock().unlock()
    } else if (siblingMap.containsKey(resId)){
      siblingWorkingSize.acquire(value)
      siblingRWLock.writeLock().lock()
      siblingMap.get(resId).getAndAdd(-value)
      siblingRWLock.writeLock().unlock()
    }
  }

  override def addResource(resId: String, defaultVal: Int): Unit = {
    addChild(resId, defaultVal)
  }

  override def require(value: Int): Unit = {

  }

  override def release(value: Int): Unit = {

  }

  override def incResource(resId: String, value: Int): Unit = {
    if(childrenMap.containsKey(resId)){
      childrenRWLock.writeLock().lock()
      childrenMap.get(resId).addAndGet(value)
      childrenRWLock.writeLock().unlock()
      childrenWorkingSize.release(value)
      if(childrenWorkingSize.availablePermits() > 0) {
        childrenNonEmpty.release()
        nonEmtpy.release()
      }
    } else if (siblingMap.containsKey(resId)){
      siblingRWLock.writeLock().lock()
      siblingMap.get(resId).addAndGet(value)
      siblingRWLock.writeLock().unlock()
      siblingWorkingSize.release(value)
      if(siblingWorkingSize.availablePermits() > 0) {
        siblingNonEmpty.release()
        nonEmtpy.release()
      }
    }
  }

  override def waitForNonEmpty(): Unit = {
    if(childrenWorkingSize.availablePermits() + siblingWorkingSize.availablePermits() < 1){
      nonEmtpy.acquire()
    }
  }

  def waitForChildrenNonEmpty(): Unit ={
    if(childrenWorkingSize.availablePermits() < 1){
      childrenNonEmpty.acquire()
    }
  }

  def waitForSiblingNonEmpty(): Unit ={
    if(siblingWorkingSize.availablePermits() < 1){
      siblingNonEmpty.acquire()
    }
  }

  def addChild(childId: String, value: Int) = {
    childrenRWLock.writeLock().lock()
    val oldValue = if(childrenMap.containsKey(childId)) childrenMap.get(childId).get() else 0
    childrenMap.put(childId, new AtomicInteger(value))
    childrenRWLock.writeLock().unlock()
    if(value > oldValue){
      childrenWorkingSize.release(value - oldValue)
      if(childrenWorkingSize.availablePermits() > 0) {
        childrenNonEmpty.release()
        nonEmtpy.release()
      }
    }
  }

  def addSibling(siblingId: String, value: Int) = {
    siblingRWLock.writeLock().lock()
    val oldValue = if(siblingMap.containsKey(siblingId)) siblingMap.get(siblingId).get() else 0
    siblingMap.put(siblingId, new AtomicInteger(value))
    siblingRWLock.writeLock().unlock()
    if(value > oldValue){
      siblingWorkingSize.release(value - oldValue)
      if(siblingWorkingSize.availablePermits() > 0) {
        siblingNonEmpty.release()
        nonEmtpy.release()
      }
    }
  }

  def removeChild(childId:String) = {
    childrenRWLock.writeLock().lock()
    childrenMap.remove(childId)
    childrenRWLock.writeLock().unlock()
  }

  def removeSibling(siblingId:String) = {
    siblingRWLock.writeLock().lock()
    siblingMap.remove(siblingId)
    siblingRWLock.writeLock().unlock()
  }

  def getAllChildrenCores():Int = {
    childrenRWLock.readLock().lock()
    val sum = if(childrenMap.isEmpty) 0
    else {
      childrenMap.values().map(_.get()).sum
    }
    childrenRWLock.readLock().unlock()
    sum
  }

  def getAllSiblingCores():Int = {
    siblingRWLock.readLock().lock()
    val sum =if(siblingMap.isEmpty) 0
    else {
      siblingMap.values().map(_.get()).sum
    }
    siblingRWLock.readLock().unlock()
    sum
  }

  def getChildrenRes():mutable.Map[String, AtomicInteger] = {
    childrenMap
  }

  def getSiblingRes():mutable.Map[String, AtomicInteger] = {
    siblingMap
  }
}
