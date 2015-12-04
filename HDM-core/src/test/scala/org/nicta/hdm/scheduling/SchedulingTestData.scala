package org.nicta.hdm.scheduling

import java.util.concurrent.atomic.AtomicInteger

import org.nicta.wdy.hdm.io.Path

import scala.collection.mutable
import scala.util.Random

/**
 * Created by tiantian on 1/09/15.
 */
trait SchedulingTestData {

  def initAddressPool(n:Int, baseHost:String = "10.10.0", port:Int = 8999) = {
    val res = mutable.Buffer.empty[String]
    for(i <- 1 to n){
      val nextIp = s"$baseHost.${Random.nextInt(256)}:$port"
      res += nextIp
    }
    res
  }

  def generateInputPath(addressPool:Seq[String], num:Int) = {
    val res = mutable.Buffer.empty[String]
    for(i <- 1 to num){
      val idx = Random.nextInt(addressPool.length)
      res += s"hdfs://${addressPool(idx)}/user/data/part-$i"
    }
    res
  }

  def generateWorkerPath(addressPool:Seq[String], factor:Int)= {
    val res = mutable.Buffer.empty[String]
    for(address <- addressPool; i <- 1 to factor){
      res += s"akka.tcp://slaveSys@${address}/user/smsSlave/"
    }
    res
  }

  def generateWorkers(addressPool:Seq[String]) = {
    addressPool.map(addr => s"akka.tcp://masterSys@$addr/user/smsSlave/")
  }

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

  val inputMap = mutable.Map.empty[Path, Int] ++= Seq[(Path, Int)](
    Path("akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster/") -> 1,
    Path("akka.tcp://masterSys@127.0.1.2:8999/user/smsMaster/") -> 2,
    Path("akka.tcp://masterSys@127.0.1.3:8999/user/smsMaster/") -> 2,
    Path("akka.tcp://masterSys@127.0.1.4:8999/user/smsMaster/") -> 3,
    Path("akka.tcp://masterSys@127.0.1.5:8999/user/smsMaster/") -> 1,
    Path("akka.tcp://masterSys@127.0.1.6:8999/user/smsMaster/") -> 3
  )

}
