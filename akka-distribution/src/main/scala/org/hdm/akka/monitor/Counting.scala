package org.hdm.akka.monitor

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.mapAsScalaConcurrentMap

import akka.actor.Actor

/**
 * 通用的用于计数的Counting工具类
 * 提供面向属性的增删改查统计方法
 * A tool class for counting
 * @author wudongyao
 * @date 2013-7-1
 * @version 0.0.1
 *
 */
trait Counting {
  val monitorDataMap = new ConcurrentHashMap[String, ConcurrentHashMap[String, AtomicLong]]() // prop ->[ key -> value]]

  def increment(prop: String, key: String, value: Long) = {
    val map = monitorDataMap.getOrElseUpdate(prop, {
      new ConcurrentHashMap[String, AtomicLong]()
    })
    map.getOrElseUpdate(key, {
      new AtomicLong(0)
    }).addAndGet(value)
  }

  def increment(prop: String, key: String) { increment(prop, key, 1) }

  def set(prop: String, key: String, value: Long) = {
    val map = monitorDataMap.getOrElseUpdate(prop, {
      new ConcurrentHashMap[String, AtomicLong]()
    })
    map.getOrElseUpdate(key, {
      new AtomicLong(0)
    }).set(value)
    this
  }

  def decrement(prop: String, key: String) = {
    val map = monitorDataMap.getOrElseUpdate(prop, {
      new ConcurrentHashMap[String, AtomicLong]()
    })
    map.getOrElseUpdate(key, {
      new AtomicLong(0)
    }).decrementAndGet()
  }

  def get(prop: String, key: String): Long = {
    monitorDataMap.get(prop) match {
      case map: ConcurrentHashMap[String, AtomicLong] => map.getOrElse(key, new AtomicLong(-1)).get()
      case _ => -2
    }
  }

  def getAll(prop: String): java.util.List[(String, Long)] = {
    val dataList = new java.util.ArrayList[(String, Long)]()
    val dataMap = monitorDataMap.getOrElse(prop, new ConcurrentHashMap[String, AtomicLong]())
    if (dataMap.size() > 0)
      dataMap.keySet().map(key => dataList.add((key, get(prop, key))))
    dataList
  }

  def allProps() = monitorDataMap.keysIterator

  def clear() = monitorDataMap.clear()

  def reset(prop: String, key: String) = set(prop, key, 0)

  def getAndReset(prop: String, key: String) = {
    val v = get(prop, key)
    reset(prop, key)
    v
  }

  def resetAll() {
    for {
      (k, m) <- monitorDataMap
      (mk, mv) <- m
    }  reset(k, mk)
  }

}

/**
 * actor Counting封装
 */
trait ActorCounting {

  self: Actor =>

  def inc(prop: String, key: String , value:Int =1) = MonitorDataExtension(context.system).increment(prop, key, value)

  def dec(prop: String, key: String) = MonitorDataExtension(context.system).decrement(prop, key)

  def get(prop: String, key: String) = MonitorDataExtension(context.system).get(prop, key)

  def clear(prop: String, key: String) = MonitorDataExtension(context.system).set(prop, key, 0)

  def getAndReset(prop: String, key: String) = MonitorDataExtension(context.system).getAndReset(prop, key)

  def getAll = MonitorDataExtension(context.system).getMonitorData(reset=false)

  def clearAll() {
    MonitorDataExtension(context.system).clear()
  }

  def getAndResetAll() = {
    val v = getAll
    clearAll()
    v
  }

}

class CountingJavaApi extends Counting {

}