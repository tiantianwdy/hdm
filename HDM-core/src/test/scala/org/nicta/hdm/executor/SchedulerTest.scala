package org.nicta.hdm.executor

import java.util.concurrent.atomic.AtomicInteger

import org.junit.Test
import org.nicta.wdy.hdm.executor.Scheduler

import scala.collection.mutable

/**
 * Created by tiantian on 28/04/15.
 */
class SchedulerTest {

  val candidateMap = mutable.Map.empty[String, AtomicInteger] ++= Seq[(String, AtomicInteger)](
    "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/" -> new AtomicInteger(1),
    "akka.tcp://masterSys@127.0.1.2:8999/user/smsMaster/" -> new AtomicInteger(2),
    "akka.tcp://masterSys@127.0.1.3:8999/user/smsMaster/" -> new AtomicInteger(2),
    "akka.tcp://masterSys@127.0.1.4:8999/user/smsMaster/" -> new AtomicInteger(3),
    "akka.tcp://masterSys@127.0.1.5:8999/user/smsMaster/" -> new AtomicInteger(1),
    "akka.tcp://masterSys@127.0.1.6:8999/user/smsMaster/" -> new AtomicInteger(3)
  )

  val partialCandidateMap = mutable.Map.empty[String, AtomicInteger] ++= Seq[(String, AtomicInteger)](
    "akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/" -> new AtomicInteger(0),
    "akka.tcp://masterSys@127.0.1.2:8999/user/smsMaster/" -> new AtomicInteger(0),
    "akka.tcp://masterSys@127.0.1.3:8999/user/smsMaster/" -> new AtomicInteger(0),
    "akka.tcp://masterSys@127.0.1.4:8999/user/smsMaster/" -> new AtomicInteger(3),
    "akka.tcp://masterSys@127.0.1.5:8999/user/smsMaster/" -> new AtomicInteger(2),
    "akka.tcp://masterSys@127.0.1.6:8999/user/smsMaster/" -> new AtomicInteger(0)
  )

  val nullCandidateMap = mutable.Map.empty[String, AtomicInteger]


  @Test
  def testFindFreestWorker(): Unit ={
    Scheduler.getFreestWorkers(candidateMap) foreach(println(_))
    Scheduler.getFreestWorkers(partialCandidateMap) foreach(println(_))
    Scheduler.getFreestWorkers(nullCandidateMap) foreach(println(_))
  }
}
