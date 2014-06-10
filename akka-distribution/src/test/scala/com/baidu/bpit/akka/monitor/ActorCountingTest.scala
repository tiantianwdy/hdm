package com.baidu.bpit.akka.monitor

import junit.framework.TestCase
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConversions._

class ActorCountingTest extends TestCase {

  val countService = new CountingJavaApi();

  private val MonitorDataMap = Map[String, Map[String, AtomicLong]]() // prop ->[ key -> value]]

  def testMapPutTest() {
    val prop: String = "test";
    println(MonitorDataMap.getOrElse(prop, {
      val map = Map[String, AtomicLong]();
      MonitorDataMap.put(prop, map)
    }))
  }

  def testJavaCounting = {

    countService.increment("sms/compressed", "13522438071", 1)
    println("1")
    countService.increment("sms/buffered", "13522438071", 1)
    println("2")
    countService.increment("sms/buffered", "13522438071", 1)
    println("3")
    println(countService.getAll("sms/compressed"));
    println(countService.getAll("sms/buffered"));
    countService.reset("sms/buffered", "13522438071")
    println(countService.getAll("sms/buffered"));
  }

}