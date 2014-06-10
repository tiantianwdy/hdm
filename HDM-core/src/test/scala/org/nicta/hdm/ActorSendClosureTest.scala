package org.nicta.hdm

import java.util.concurrent.{Executors, ExecutorService}

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt

import akka.actor.{ActorSystem, Props, actorRef2Scala}
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout

import org.junit.Test
import org.nicta.wdy.hdm.ClosureCleaner

/**
 * Created by Tiantian on 2014/5/27.
 */
class ActorSendClosureTest {

  @Test
  def testMyActor() {
    val f: String => Array[String] = _.split(""",""")
    val f2: List[String] => List[Any] = _.flatMap(s => s.split(","))
    val executor: ExecutorService = Executors.newFixedThreadPool(4)
    implicit val ec = ExecutionContext.fromExecutorService(executor)
    implicit val timeout = Timeout(5 seconds)
    val foo = Promise.successful("foo")
    val system = ActorSystem("scalaActorTest")
    val myActor = system.actorOf(Props[MyActor], name = "myActor")
    ClosureCleaner.apply(f)
    myActor ! 100
    myActor ! "test text"
    myActor ! "msg"
    myActor ! f
    myActor ! f2
    val f1: Future[String] = ask(myActor, "askMsg").mapTo[String]
    f1 pipeTo myActor

    Thread.sleep(2000)
    //  system.stop(myActor)
  }

}
