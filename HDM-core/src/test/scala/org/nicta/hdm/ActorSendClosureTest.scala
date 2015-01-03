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
    val f: Iterator[String] => List[Array[String]] = _.map(s => s.split(""",""")).toList
    val f2: Iterator[String] => Iterator[Double] = _.flatMap(s => s.split(""",""")).map(_.toDouble)

    val executor: ExecutorService = Executors.newFixedThreadPool(4)
    implicit val ec = ExecutionContext.fromExecutorService(executor)
    implicit val timeout = Timeout(5 seconds)

    val foo = Promise.successful("foo")
    val system = ActorSystem("scalaActorTest")
    val myActor = system.actorOf(Props[MyActor], name = "myActor")
    // functional packing
    ClosureCleaner.apply(f2)
    val func = f2.asInstanceOf[Iterator[_] => _]

//    myActor ! 100
//    myActor ! "test text"
//    myActor ! "msg"
    myActor ! FuncTask(None,func)
    val f1: Future[String] = ask(myActor, "askMsg").mapTo[String]
    f1 pipeTo myActor

    Thread.sleep(2000)
    //  system.stop(myActor)
  }

}


case class FuncTask[T,U] (val context: Any, val func: Iterator[T] => U ) {



}