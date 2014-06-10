package com.baidu.bpit.akka

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import scala.Array.canBuildFrom
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt

import akka.actor.{Address, ActorSystem, Props, actorRef2Scala}
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import junit.framework.TestCase

class BasicTest extends TestCase {

  def testNetInfoRegex() {
    println("  eth0:11829526151 124497139    0    0    0     0          0         0 14457894904 104015553    0    0    0     0       0          0".split("[(\\s):]+").map(_ match {
      case s: String if (s.matches("\\d+")) => s.toLong
      case _ => 0
    }).toList)
  }


  def testReflect() {
    val system = ActorSystem()
    val clazzName = "com.baidu.bpit.akka.MyActor"
    val clazz = Class.forName(clazzName)
    val actor = system.actorOf(Props(clazz), "reflectActor")
    println(actor.path.toString)
    actor ! 100
    Thread.sleep(2000)
  }

  def testMyActor() {
    val f: String => Array[String] = _.split("|")
    val executor: ExecutorService = Executors.newFixedThreadPool(4)
    implicit val ec = ExecutionContext.fromExecutorService(executor)
    implicit val timeout = Timeout(5 seconds)
    val foo = Promise.successful("foo")
    val system = ActorSystem("scalaActorTest")
    val myActor = system.actorOf(Props[MyActor], name = "myActor")
    myActor ! 100
    myActor ! "test text"
    myActor ! "msg"
    myActor ! f
    val f1: Future[String] = ask(myActor, "askMsg").mapTo[String]
    f1 pipeTo myActor

    Thread.sleep(2000)
    //  system.stop(myActor)
  }

  def testString() {
    val ss = s"sdhsjkdhsj${System.currentTimeMillis()}"
  }


  def testLongTime() {
    println(System.currentTimeMillis() - 420000)
    println(System.currentTimeMillis() - 120000)
  }

}