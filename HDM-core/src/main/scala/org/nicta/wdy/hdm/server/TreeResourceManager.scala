package org.nicta.wdy.hdm.server

import java.util.concurrent.{Semaphore, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by tiantian on 9/05/16.
 */
class TreeResourceManager extends DefResourceManager{

  val childrenMap: java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]

  val siblingMap: java.util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]


  override def init(): Unit = {
    super.init()
    childrenMap.clear()
    siblingMap.clear()
  }

  def addChild(childId: String, value: Int) = {
    childrenMap.put(childId, new AtomicInteger(value))
  }

  def addSibling(siblingId: String, value: Int) = {
    siblingMap.put(siblingId, new AtomicInteger(value))
  }

  def removeChild(childId:String) = {
    childrenMap.remove(childId)
  }

  def removeSibling(siblingId:String) = {
    siblingMap.remove(siblingId)
  }

  def getAllChildrenCores():Int = {
    if(childrenMap.isEmpty) 0
    else {
      childrenMap.values().map(_.get()).sum
    }
  }

  def getAllSiblingCores():Int = {
    if(siblingMap.isEmpty) 0
    else {
      siblingMap.values().map(_.get()).sum
    }
  }

  def getChildrenRes():mutable.Map[String, AtomicInteger] = {
    childrenMap
  }

  def getSiblingRes():mutable.Map[String, AtomicInteger] = {
    siblingMap
  }

}
